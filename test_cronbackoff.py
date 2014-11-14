"""
Test suite for cronbackoff.py

To be run through nose (https://nose.readthedocs.org/), not directly
"""

import logging
import unittest

import cronbackoff

class TestParseArgs(unittest.TestCase):
  def test_basic(self):
    prog = "wefa"
    base_delay = 10
    max_delay = 10000
    exponent = 22.1222332312
    name = "asdf"
    command = ["as23d", 1, 4, "hi", "-h"]
    opts = cronbackoff.parseArgs(
        [prog,
          "-b", str(base_delay),
          "--max-delay", str(max_delay),
          "-e", str(exponent),
          "--debug",
          "--name", name,
          "--",
          ] + command)

    self.assertEqual(opts.base_delay, base_delay)
    self.assertEqual(opts.max_delay, max_delay)
    self.assertAlmostEqual(opts.exponent, exponent)
    self.assertEqual(opts.debug, True)
    self.assertEqual(opts.name, name)
    self.assertEqual(opts.command, command)

  def test_defaults(self):
    prog = "nosetests"
    name = "hurkle"
    command = "/usr/bin/%s" % name
    opts = cronbackoff.parseArgs(
        [prog, command])

    self.assertEqual(opts.base_delay, 60)
    self.assertEqual(opts.max_delay, 1440)
    self.assertAlmostEqual(opts.exponent, 4)
    self.assertEqual(opts.debug, False)
    self.assertEqual(opts.name, name)
    self.assertEqual(opts.command, [command])
