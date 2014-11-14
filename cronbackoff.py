#!/usr/bin/python -tt

import argparse
import errno
import fcntl
import logging
import os
import pwd
import subprocess
import stat
import sys
import time
import tempfile

stateFile = None
stateExists = True
user = pwd.getpwuid(os.getuid())[0]
lastRun = None
lastDelay = None
nextRun = None
nextDelay = 0

def main():
  setupLogging()
  opts = parseArgs(sys.argv)
  mkStateDir(opts.state_dir)
  getLock(opts.state_dir, opts.name)
  readState()
  backoff()
  success = execute(opts.command)
  saveState(success, opts)
  cleanupExit(0)

def setupLogging():
  logging.basicConfig(
      format='%(asctime)s %(name)s(%(levelname)s): %(message)s',
      datefmt="%Y-%m-%d %H:%M:%S")

def _getLogger():
  return logging.getLogger()

def parseArgs(args):
  prog = os.path.basename(args[0])
  parser = argparse.ArgumentParser(prog=prog)

  parser.add_argument("-b", "--base-delay", default=60, type=int,
      help=("Time (in minutes) to skip execution after the first failure"
            " (Default: %(default)s mins)"))
  parser.add_argument("-m", "--max-delay", default=(60 * 24), type=int,
      help=("Maximum time (in minutes) to skip execution"
            " (Default: %(default)s mins)"))
  parser.add_argument("-e", "--exponent", default=4, type=float,
      help=("How much to multiply the previous delay upon another failure"
            " (Default: %(default)sx)"))
  parser.add_argument("-d", "--debug", action='store_true',
      help="Enable debugging output")
  parser.add_argument("-n", "--name", default=None,
      help="Name of state file. Defaults to name of command")
  parser.add_argument("--state-dir",
      default=os.path.join(tempfile.gettempdir(), "%s-%s" % (prog, user)),
      help="Directory to store state in (Default: %(default)s)")
  parser.add_argument("command", nargs="+",
      help="Command to run")
  opts = parser.parse_args(args=args[1:])

  if opts.name is None:
    opts.name = os.path.basename(opts.command[0])
  opts.state_dir = os.path.expanduser(opts.state_dir)

  logger = _getLogger()
  logger.name = prog
  if opts.debug:
    logger.setLevel(logging.DEBUG)

  logging.info("Options: %s", opts)

  return opts

def mkStateDir(state_dir):
  try:
    os.mkdir(state_dir, 0700)
  except OSError as e:
    if e.errno == errno.EEXIST:
      logging.debug("State dir (%s) already exists", state_dir)
    else:
      logging.critical("Unable to make state dir: %s", e.strerror)
      cleanupExit(1)
  else:
    logging.debug("State dir (%s) created", state_dir)

  st = os.lstat(state_dir)
  errs = []
  if not stat.S_ISDIR(st.st_mode):
    errs.append("not a directory")
  if stat.S_ISLNK(st.st_mode):
    errs.append("a symlink")
  if st.st_uid != os.getuid():
    errs.append("not owned by current user")
  if st.st_gid != os.getgid():
    errs.append("not owned by current group")
  if errs:
    logging.critical("State dir (%s) is: %s", state_dir, ", ".join(errs))
    cleanupExit(1)

def getLock(state_dir, name):
  global stateFile, stateExists
  path = os.path.join(state_dir, name)
  logging.debug("Opening state file (%s)", path)

  try:
    stateFile = open(path, 'r+')
  except IOError as e:
    if e.errno == errno.ENOENT:
      stateExists = False
      logging.debug("State file doesn't exist")
    else:
      logging.critical("Unable to open state file (%s): %s", path, e.strerror)
      cleanupExit(1)

  if not stateExists:
    logging.debug("Creating new state file")
    try:
      stateFile = open(path, 'w+')
    except IOError as e:
      logging.critical("Unable to create state file (%s): %s", path, e.strerror)
      cleanupExit(1)

  logging.debug("Locking state file")
  try:
    fcntl.lockf(stateFile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  except IOError as e:
    if e.errno in [errno.EACCES, errno.EAGAIN]:
      logging.critical("State file (%s) already locked", path)
    else:
      logging.critical("Unable to lock state file (%s): %s", path, e.strerror)
    cleanupExit(1)
  logging.debug("State file opened & locked")

def readState():
  global lastRun, lastDelay, nextRun

  if not stateExists:
    logging.info("No existing state")
    return

  logging.debug("Stat'ing state file")
  st = os.fstat(stateFile.fileno())
  lastRun = st.st_mtime
  logging.info("Last run finished: %s (%s ago)",
      time.ctime(lastRun), formatTime(time.time() - lastRun))

  contents = stateFile.read()
  logging.debug("State file contents: %r", contents)

  try:
    lastDelay = int(contents)
  except ValueError:
    logging.critical("Corrupt state file - %r is not a valid integer", contents)
    cleanupExit(1)
  nextRun = lastRun + (lastDelay * 60)
  if lastDelay == 0:
    logging.info("No previous backoff")
  else:
    logging.info("Last backoff (%s) was until %s",
        formatTime(lastDelay * 60, precision="minutes"),
        time.ctime(nextRun))

def formatTime(seconds, precision="seconds"):
  out = []
  m, s = divmod(seconds, 60)
  h, m = divmod(m, 60)
  if h and precision in ["hours", "minutes", "seconds"]:
    out.append("%dh" % h)
  if m and precision in ["minutes", "seconds"]:
    out.append("%dm" % m)
  if s and precision in ["seconds"]:
    out.append("%ds" % round(s))
  if not out:
    if precision == "hours":
      out.append("0h")
    elif precision == "minutes":
      out.append("0m")
    elif precision == "seconds":
      out.append("0s")
  return " ".join(out)

def backoff():
  now = time.time()
  if not stateExists:
    logging.info("No existing state, execute command")
    return
  if lastDelay == 0:
    logging.info("Not in backoff, execute command")
    return
  if nextRun > now:
    logging.info("Still in backoff for another %s, skipping execution.", formatTime(nextRun - now))
    cleanupExit(0)
  logging.info("No longer in backoff, execute command")

def execute(command):
  logging.info("About to execute command: %s", " ".join(command))
  logging.debug("Raw command: %r", command)
  success = True
  try:
    output = subprocess.check_output(command, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as e:
    logging.warning("Command exited with non-zero return status: %d", e.returncode)
    logging.info("Command output:")
    for line in e.output.splitlines():
      logging.info("    %s", line)
    success = False
  except OSError as e:
    logging.critical("Error running command: %s", e.strerror)
    cleanupExit(1)
  else:
    logging.info("Command exited cleanly")
    logging.debug("Command output:")
    for line in output.splitlines():
      logging.debug("    %s", line)

  return success

def saveState(success, opts):
  global stateFile, nextDelay

  if success:
    logging.info("Execution successful, no backoff")
    nextDelay = 0
  else:
    if not lastDelay: # Works if lastDelay was 0, or is unset due to no preexisting state
      nextDelay = opts.base_delay
    else:
      nextDelay = min(lastDelay * opts.exponent, opts.max_delay)

  stateFile.seek(0)
  stateFile.truncate(0)
  stateFile.write("%d\n" % nextDelay)
  stateFile.flush()
  st = os.fstat(stateFile.fileno())
  fcntl.lockf(stateFile.fileno(), fcntl.LOCK_UN)
  stateFile.close()
  stateFile = None

  if nextDelay:
    logging.warning("Execution unclean, backoff delay is %s (until %s)",
        formatTime(nextDelay * 60), time.ctime(st.st_mtime + nextDelay * 60))

def cleanupExit(status):
  if not stateExists and stateFile:
    # If there wasn't an existing state file, and it hasn't been closed already,
    # that means we've created an empty one, so unlink it.
    os.unlink(stateFile.name)
    fcntl.lockf(stateFile.fileno(), fcntl.LOCK_UN)
    stateFile.close()

  if status == 0:
    logging.debug("Exiting (%d)", status)
  else:
    logging.critical("Exiting (%d)", status)

  sys.exit(status)

if __name__ == '__main__':
  main()
