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

user = pwd.getpwuid(os.getuid())[0]
state = None

def main():
  global state
  setupLogging()
  opts = parseArgs(sys.argv)
  state = State(opts.state_dir, opts.name)
  state.mkStateDir()
  state.getLock()
  state.read()
  state.backoff()
  success = execute(opts.command)
  state.save(success, opts.base_delay, opts.max_delay, opts.exponent)
  cleanupExit(0)

def setupLogging():
  logging.basicConfig(
      format='%(asctime)s %(name)s(%(levelname)s): %(message)s',
      datefmt="%Y-%m-%d %H:%M:%S")

def _getLogger():
  return logging.getLogger()

def parseArgs(args):
  prog = os.path.basename(args[0])
  bareProg = os.path.splitext(prog)[0]
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
      default=os.path.join(tempfile.gettempdir(), "%s-%s" % (bareProg, user)),
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

def cleanupExit(status):
  if not state.stateExists and state.file:
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


class State(object):
  def __init__(self, dir_, name):
    self.dir = dir_
    self.name = name
    self.filePath = os.path.join(self.dir, self.name)
    self.file = None
    self.stateExists = True

    self.lastRun = None
    self.lastDelay = None
    self.nextRun = None

  def mkStateDir(self):
    try:
      os.mkdir(self.dir, 0700)
    except OSError as e:
      if e.errno == errno.EEXIST:
        logging.debug("State dir (%s) already exists", self.dir) # Deliberately broken, for testing tests.
      else:
        logging.critical("Unable to make state dir: %s", e.strerror)
        cleanupExit(1)
    else:
      logging.debug("State dir (%s) created", self.dir) # ditto

    st = os.lstat(self.dir)
    errs = []
    if stat.S_ISLNK(st.st_mode):
      errs.append("a symlink")
    if not stat.S_ISDIR(st.st_mode):
      errs.append("not a directory")
    if st.st_uid != os.getuid():
      errs.append("not owned by current user")
    if st.st_gid != os.getgid():
      errs.append("not owned by current group")
    if errs:
      logging.critical("State dir (%s) is: %s", dir, ", ".join(errs))
      cleanupExit(1)

  def getLock(self):
    logging.debug("Opening state file (%s)", self.filePath)

    try:
      self.file = open(self.filePath, 'r+')
    except IOError as e:
      if e.errno == errno.ENOENT:
        self.stateExists = False
        logging.debug("State file doesn't exist")
      else:
        logging.critical("Unable to open state file (%s): %s", self.filePath, e.strerror)
        cleanupExit(1)
    else:
      logging.debug("State file already exists")

    if not self.stateExists:
      logging.debug("Creating new state file")
      try:
        self.file = open(self.filePath, 'w+')
      except IOError as e:
        logging.critical("Unable to create state file (%s): %s", self.filePath, e.strerror)
        cleanupExit(1)

    logging.debug("Locking state file")
    try:
      fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as e:
      if e.errno in [errno.EACCES, errno.EAGAIN]:
        logging.critical("State file (%s) already locked", self.filePath)
      else:
        logging.critical("Unable to lock state file (%s): %s", self.filePath, e.strerror)
      cleanupExit(1)
    logging.debug("State file opened & locked")

  def read(self):
    if not self.stateExists:
      logging.info("No existing state")
      return

    logging.debug("Stat'ing state file")
    st = os.fstat(self.file.fileno())
    self.lastRun = st.st_mtime
    logging.info("Last run finished: %s (%s ago)",
        time.ctime(self.lastRun), formatTime(time.time() - self.lastRun))

    contents = self.file.read()
    logging.debug("State file contents: %r", contents)

    try:
      self.lastDelay = int(contents)
    except ValueError:
      logging.critical("Corrupt state file - %r is not a valid integer", contents)
      cleanupExit(1)
    self.nextRun = self.lastRun + (self.lastDelay * 60)
    if self.lastDelay == 0:
      logging.info("No previous backoff")
    else:
      logging.info("Last backoff (%s) was until %s",
          formatTime(self.lastDelay * 60, precision="minutes"),
          time.ctime(self.nextRun))

  def backoff(self):
    now = time.time()
    if not self.stateExists:
      logging.info("No existing state, execute command")
      return
    if self.lastDelay == 0:
      logging.info("Not in backoff, execute command")
      return
    if self.nextRun > now:
      logging.info("Still in backoff for another %s, skipping execution.", formatTime(self.nextRun - now))
      cleanupExit(0)
    logging.info("No longer in backoff, execute command")

  def save(self, success, base_delay, max_delay, exponent):
    if success:
      logging.info("Execution successful, no backoff")
      nextDelay = 0
    else:
      if not self.lastDelay: # Works if lastDelay was 0, or is unset due to no preexisting state
        nextDelay = min(base_delay, max_delay) # max_delay wins over base_delay
      else:
        nextDelay = min(self.lastDelay * exponent, max_delay)

    try:
      self.file.seek(0)
      self.file.truncate(0)
      self.file.write("%d\n" % nextDelay)
      self.file.flush()
      self.file.close()
      self.file = None
    except IOError as e:
      logging.critical("Unable to write state file (%s): %s", self.filePath, e.strerror)
      cleanupExit(1)

    st = os.lstat(self.filePath)
    if nextDelay:
      logging.warning("Execution unclean, backoff delay is %s (until %s)",
          formatTime(nextDelay * 60), time.ctime(st.st_mtime + nextDelay * 60))


if __name__ == '__main__':
  main()
