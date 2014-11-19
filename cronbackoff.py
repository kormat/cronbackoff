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

def main():
  global state
  try:
    _setupLogging()
    opts = _parseArgs(sys.argv)
    state = State(opts.state_dir, opts.name)
    state.mkStateDir()
    state.getLock()
    state.read()
    delay = state.backoff()
    if delay and delay <= 0:
      success = execute(opts.command)
      state.save(success, opts.base_delay, opts.max_delay, opts.exponent)
  except CronBackoffException as e:
    if e.status == 0 :
      logging.debug("Exiting (%d)", e.status)
    else:
      logging.critical(e.message)
      logging.critical("Exiting (%d)", e.status)
    sys.exit(e.status)
  except KeyboardInterrupt as e:
    logging.error("Caught keyboard interruption")
    logging.error("Exiting (1)")
    sys.exit(1)
  except:
    logging.critical("Unexpected error:", exc_info=True)
    logging.critical("Exiting (1)")
    sys.exit(1)
  finally:
    # If there wasn't an existing state file, and it hasn't been closed already,
    # that means we've created an empty one, so unlink it.
    if not state.stateExists and state.file:
      os.unlink(state.filePath)
      state.file.close()
  logging.debug("Exiting (0)")
  sys.exit(0)

def _setupLogging():
  logging.basicConfig(
      format='%(asctime)s %(name)s(%(levelname)s): %(message)s',
      datefmt="%Y-%m-%d %H:%M:%S")

def _getLogger():
  return logging.getLogger()

def _parseArgs(args):
  prog = os.path.basename(args[0])
  bareProg = os.path.splitext(prog)[0]
  parser = argparse.ArgumentParser(prog=prog)
  user = pwd.getpwuid(os.getuid())[0]

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

def _formatTime(seconds, precision="seconds"):
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
    logging.warning(e)
    logging.info("Command output:")
    for line in e.output.splitlines():
      logging.info("    %s", line)
    success = False
  except OSError as e:
    raise CronBackoffException(
        "Error running command %r: %s" % (command, e),
        excep=e)
  else:
    logging.info("Command exited cleanly")
    logging.debug("Command output:")
    for line in output.splitlines():
      logging.debug("    %s", line)

  return success


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
    logging.debug("Creating state dir (%s)", self.dir)

    try:
      os.mkdir(self.dir, 0700)
    except OSError as e:
      if e.errno == errno.EEXIST:
        logging.debug("State dir already exists")
      else:
        raise CronBackoffException("Unable to make state dir: %s" % e, excep=e)
    else:
      logging.debug("State dir (%s) created", self.dir)

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
      raise CronBackoffException("State dir (%s) is: %s" % (self.dir, ", ".join(errs)))

  def getLock(self):
    logging.debug("Opening state file (%s)", self.filePath)

    try:
      self.file = open(self.filePath, 'r+')
    except IOError as e:
      if e.errno == errno.ENOENT:
        self.stateExists = False
        logging.debug("State file doesn't exist")
      else:
        raise CronBackoffException("Unable to open state file: %s" % e, excep=e)
    else:
      logging.debug("State file already exists")

    if not self.stateExists:
      logging.debug("Creating new state file")
      try:
        self.file = open(self.filePath, 'w+')
      except IOError as e:
        raise CronBackoffException("Unable to create state file: %s" % e, excep=e)

    logging.debug("Locking state file")
    try:
      fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as e:
      raise CronBackoffException("Unable to lock state file (%s): %s" % (self.filePath, e), excep=e)
    logging.debug("State file opened & locked")

  def read(self):
    if not self.stateExists:
      logging.info("No existing state")
      return

    logging.debug("Stat'ing state file")
    st = os.fstat(self.file.fileno())
    self.lastRun = st.st_mtime
    logging.info("Last run finished: %s (%s ago)",
        time.ctime(self.lastRun), _formatTime(time.time() - self.lastRun))

    try:
      contents = self.file.read()
    except IOError as e:
      raise CronBackoffException("Unable to read state file: %s" % e, excep=e)
    logging.debug("State file contents: %r", contents)

    try:
      self.lastDelay = int(contents)
    except ValueError as e:
      raise CronBackoffException("Corrupt state file - not a valid integer: %r" % contents,
          excep=e)
    self.nextRun = self.lastRun + (self.lastDelay * 60)
    if self.lastDelay == 0:
      logging.info("No previous backoff")
    else:
      logging.info("Last backoff (%s) was until %s",
          _formatTime(self.lastDelay * 60, precision="minutes"),
          time.ctime(self.nextRun))

  def backoff(self):
    delay = None
    now = time.time()
    if not self.stateExists:
      delay = None
      logging.info("No existing state, execute command")
    elif self.lastDelay == 0:
      delay = 0
      logging.info("Not in backoff, execute command")
    elif self.nextRun > now:
      delay = self.nextRun - now
      logging.info("Still in backoff for another %s, skipping execution.", _formatTime(delay))
    else:
      delay = -self.lastDelay
      logging.info("No longer in backoff, execute command")
    return delay

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
      raise CronBackoffException("Unable to write state file: %s" % e, excep=e)

    st = os.lstat(self.filePath)
    if nextDelay:
      logging.warning("Execution unclean, backoff delay is %s (until %s)",
          _formatTime(nextDelay * 60), time.ctime(st.st_mtime + nextDelay * 60))


class CronBackoffException(Exception):
  def __init__(self, message, excep=None, status=1):
    self.excep = excep
    self.errno = None
    self.status = status
    baseArg = message

    if self.excep is not None:
      self.errno = getattr(self.excep, 'errno', None)

    super(CronBackoffException, self).__init__(baseArg)

if __name__ == '__main__':
  main()
