#!/usr/bin/python

import argparse
import errno
import fcntl
import logging
import os
import pwd
import stat
import sys
import tempfile

PROG = "cronbackoff"
opts = None
logger = None
stateFile = None
user = pwd.getpwuid(os.getuid())[0]

def main():
  setupLogging()
  parseArgs()
  mkStateDir()
  getLock()

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
    sys.exit(1)

def getLock():
  pass

if __name__ == '__main__':
  main()
