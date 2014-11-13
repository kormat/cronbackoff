#!/usr/bin/python

import argparse
import errno
import fcntl
import logging
import os
import pwd
import stat
import sys
import time
import tempfile

PROG = "cronbackoff"
opts = None
logger = None
stateFile = None
user = pwd.getpwuid(os.getuid())[0]
lastRun = None
lastDelay = None

def main():
  setupLogging()
  parseArgs()
  mkStateDir()
  getLock()
  readState()

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
      help="Time (in minutes) to skip execution after the first failure (Default: %(default)s mins)")
  parser.add_argument("-m", "--max-delay", default=(60 * 24), type=int,
      help="Maximum time (in minutes) to skip execution (Default: %(default)s mins)")
  parser.add_argument("-e", "--exponent", default=4, type=float,
      help="How much to multiply the previous delay upon another failure (Default: %(default)sx)")
  parser.add_argument("-d", "--debug", action='store_true', help="Enable debugging output")
  parser.add_argument("-n", "--name", default=None, help="Name of state file. Defaults to name of command")
  parser.add_argument("--state-dir", default=os.path.join(tempfile.gettempdir(), "%s-%s" % (PROG, user)),
      help="Directory to store state in (Default: %(default)s)")
  parser.add_argument("command", nargs="+",
      help="Command to run")
  opts = parser.parse_args()

  if opts.name is None:
    opts.name = os.path.basename(opts.command[0])

  if opts.debug:
    logger.setLevel(logging.DEBUG)

  logger.info("Options: %s", opts)

def mkStateDir():
  # Try to make directory. If that fails, check the existing file.
  created = True
  try:
    os.mkdir(opts.state_dir, 0700)
  except OSError as e:
    if e.errno == errno.EEXIST:
      logging.debug("State dir (%s) already exists" % opts.state_dir)
      created = False
    else:
      raise
  else:
    logging.debug("State dir (%s) created" % opts.state_dir)
    return

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
    logging.critical("State dir (%s) is: %s" % (opts.state_dir, ", ".join(errs)))
    logging.critical("Exiting")
    sys.exit(1)

def getLock():
  global stateFile
  path = os.path.join(opts.state_dir, opts.name)
  logging.debug("Opening state file (%s)" % path)
  try:
    stateFile = open(path, 'r+')
  except IOError as e:
    logging.critical("Unable to open state file (%s):" % path)
    raise

  logging.debug("Locking state file")
  try:
    fcntl.lockf(stateFile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  except IOError as e:
    if e.errno in [errno.EACCES, errno.EAGAIN]:
      logging.critical("State file (%s) already locked" % path)
      logging.critical("Exiting")
      sys.exit(1)
    else:
      logging.critical("Unable to lock state file (%s):" % path)
      raise
  logging.debug("State file opened & locked")

def readState():
  global lastRun, lastDelay
  logging.debug("Stating state file")
  st = os.fstat(stateFile.fileno())
  lastRun = st.st_mtime
  now = time.time()
  logging.info("Last run finished: %s (%s ago)", time.ctime(lastRun), formatTime(now - lastRun))

  contents = stateFile.read()
  logging.debug("State file contents: %r", contents)

  try:
    lastDelay = int(contents)
  except ValueError:
    logging.critical("Corrupt state file - %r is not a valid integer", contents)
    logging.critical("Exiting")
    sys.exit(1)
  logging.info("Last delay: %s", formatTime(lastDelay * 60, precision="minutes"))

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

if __name__ == '__main__':
  main()
