#!/usr/bin/python

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

PROG = "cronbackoff"
opts = None
logger = None
stateFile = None
stateExists = True
user = pwd.getpwuid(os.getuid())[0]
lastRun = None
lastDelay = None
nextRun = None
nextDelay = 0

def main():
  setupLogging()
  parseArgs()
  mkStateDir()
  getLock()
  readState()
  backoff()
  success = execute()
  saveState(success)
  cleanupExit(0)

def setupLogging():
  global logger
  logging.basicConfig(
      format='%(asctime)s %(name)s(%(levelname)s): %(message)s',
      datefmt="%Y-%m-%d %H:%M:%S")
  logger = logging.getLogger()
  logger.name = PROG

def parseArgs():
  global opts
  parser = argparse.ArgumentParser()

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
      default=os.path.join(tempfile.gettempdir(), "%s-%s" % (PROG, user)),
      help="Directory to store state in (Default: %(default)s)")
  parser.add_argument("command", nargs="+",
      help="Command to run")
  opts = parser.parse_args()

  if opts.name is None:
    opts.name = os.path.basename(opts.command[0])
  opts.state_dir = os.path.expanduser(opts.state_dir)

  if opts.debug:
    logger.setLevel(logging.DEBUG)

  logger.info("Options: %s", opts)

def mkStateDir():
  try:
    os.mkdir(opts.state_dir, 0700)
  except OSError as e:
    if e.errno == errno.EEXIST:
      logger.debug("State dir (%s) already exists", opts.state_dir)
    else:
      logging.critical("Unable to make state dir: %s", e.strerror)
      cleanupExit(1)
  else:
    logger.debug("State dir (%s) created", opts.state_dir)

  st = os.lstat(opts.state_dir)
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
    logger.critical("State dir (%s) is: %s", opts.state_dir, ", ".join(errs))
    cleanupExit(1)

def getLock():
  global stateFile, stateExists
  path = os.path.join(opts.state_dir, opts.name)
  logger.debug("Opening state file (%s)", path)

  try:
    stateFile = open(path, 'r+')
  except IOError as e:
    if e.errno == errno.ENOENT:
      stateExists = False
      logger.debug("State file doesn't exist")
    else:
      logger.critical("Unable to open state file (%s): %s", path, e.strerror)
      cleanupExit(1)

  if not stateExists:
    logger.debug("Creating new state file")
    try:
      stateFile = open(path, 'w+')
    except IOError as e:
      logger.critical("Unable to create state file (%s): %s", path, e.strerror)
      cleanupExit(1)

  logger.debug("Locking state file")
  try:
    fcntl.lockf(stateFile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  except IOError as e:
    if e.errno in [errno.EACCES, errno.EAGAIN]:
      logger.critical("State file (%s) already locked", path)
    else:
      logger.critical("Unable to lock state file (%s): %s", path, e.strerror)
    cleanupExit(1)
  logger.debug("State file opened & locked")

def readState():
  global lastRun, lastDelay, nextRun

  if not stateExists:
    logger.info("No existing state")
    return

  logger.debug("Stat'ing state file")
  st = os.fstat(stateFile.fileno())
  lastRun = st.st_mtime
  logger.info("Last run finished: %s (%s ago)",
      time.ctime(lastRun), formatTime(time.time() - lastRun))

  contents = stateFile.read()
  logger.debug("State file contents: %r", contents)

  try:
    lastDelay = int(contents)
  except ValueError:
    logger.critical("Corrupt state file - %r is not a valid integer", contents)
    cleanupExit(1)
  nextRun = lastRun + (lastDelay * 60)
  if lastDelay == 0:
    logger.info("No previous backoff")
  else:
    logger.info("Last backoff (%s) was until %s",
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
  if not stateExists:
    logger.info("No existing state, execute command")
    return
  if lastDelay == 0:
    logger.info("Not in backoff, execute command")
    return
  if nextRun > time.time():
    logger.info("Still in backoff for another %s, skipping execution.", formatTime(nextRun - now))
    cleanupExit(0)
  logger.info("No longer in backoff, execute command")

def execute():
  logger.info("About to execute command: %s", " ".join(opts.command))
  logger.debug("Raw command: %r", opts.command)
  success = True
  try:
    output = subprocess.check_output(opts.command, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as e:
    logger.warning("Command exited with non-zero return status: %d", e.returncode)
    logger.info("Command output:")
    for line in e.output.splitlines():
      logger.info("    %s", line)
    success = False
  except OSError as e:
    logger.critical("Error running command: %s", e.strerror)
    cleanupExit(1)
  else:
    logger.info("Command exited cleanly")
    logger.debug("Command output:")
    for line in output.splitlines():
      logger.debug("    %s", line)

  return success

def saveState(success):
  global stateFile, nextDelay

  if success:
    logger.info("Execution successful, no backoff")
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
    logger.warning("Execution unclean, backoff delay is %s (until %s)",
        formatTime(nextDelay * 60), time.ctime(st.st_mtime + nextDelay * 60))

def cleanupExit(status):
  if not stateExists and stateFile:
    # If there wasn't an existing state file, and it hasn't been closed already,
    # that means we've created an empty one, so unlink it.
    os.unlink(stateFile.name)
    fcntl.lockf(stateFile.fileno(), fcntl.LOCK_UN)
    stateFile.close()

  if status == 0:
    logging.debug("Exiting (%d)", exit)
  else:
    logging.critical("Exiting (%d)", exit)

  sys.exit(exit)

if __name__ == '__main__':
  main()
