#!/usr/bin/python

import argparse
import os
import fcntl
import tempfile

PROG = "cronbackoff"
opts = None

def main():
  parseArgs()
  getLock()

def parseArgs():
  global opts
  parser = argparse.ArgumentParser()

  parser.add_argument("-b", "--base-delay", default=60, type=int,
      help="Time (in minutes) to skip execution after the first failure (Default: %(default)s mins)")
  parser.add_argument("-m", "--max-delay", default=(60 * 24), type=int,
      help="Maximum time (in minutes) to skip execution (Default: %(default)s mins)")
  parser.add_argument("-e", "--exponent", default=4, type=float,
      help="How much to multiply the previous delay upon another failure (Default: %(default)sx)")
  parser.add_argument("-d", "--debug", action='store_true', help="Debugging output")
  parser.add_argument("--state-dir", default=os.path.join(tempfile.gettempdir(), PROG),
      help="Directory to store state in (Default: %(default)s)")
  parser.add_argument("command", nargs=argparse.REMAINDER,
      help="Command to run")
  opts = parser.parse_args()

  if opts.debug:
    print "Options:", opts

def getLock():

  pass

if __name__ == '__main__':
  main()
