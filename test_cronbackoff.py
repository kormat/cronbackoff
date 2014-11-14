"""
Test suite for cronbackoff.py

To be run through nose (https://nose.readthedocs.org/), not directly
"""

import logging
import os
import sys
import tempfile
import unittest

import cronbackoff

class TestParseArgs(unittest.TestCase):
  def test_basic(self):
    prog = "wefa"
    base_delay = 10
    max_delay = 10000
    exponent = 22.1222332312
    name = "asdf"
    command = ["as23d", "1", "4", "hi", "-h"]
    args = [
        prog,
        "-b", str(base_delay),
        "--max-delay", str(max_delay),
        "-e", str(exponent),
        "--debug",
        "--name", name,
        "--",
        ] + command
    opts = cronbackoff.parseArgs(args)

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

class TestMkStateDir(unittest.TestCase):
  def setUp(self):
    self._cleanupCalled = False
    self._orig_cleanupExit = cronbackoff.cleanupExit
    cronbackoff.cleanupExit = self._mock_cleanupExit

  def tearDown(self):
    cronbackoff.cleanupExit = self._orig_cleanupExit

  def _mock_cleanupExit(self, state):
    self._cleanupCalled = state

  def test_dir_exists(self):
    tempDir = tempfile.mkdtemp(prefix="test_cronbackoff.")
    cronbackoff.mkStateDir(tempDir)
    self.assertEqual(self._cleanupCalled, False)
    os.rmdir(tempDir)

  def test_new_dir(self):
    tempDir = tempfile.mkdtemp(prefix="test_cronbackoff.")
    stateDir = os.path.join(tempDir, "subdir")
    cronbackoff.mkStateDir(stateDir)
    self.assertTrue(os.path.isdir(stateDir))
    self.assertEqual(self._cleanupCalled, False)
    os.rmdir(stateDir)
    os.rmdir(tempDir)

  def test_dir_wrong_owner(self):
    # It's non-trivial to create dir with wrong owner, so just use one that's guaranteed to exist.
    cronbackoff.mkStateDir("/")
    self.assertEqual(self._cleanupCalled, 1)

  def test_file(self):
    cronbackoff.mkStateDir("/etc/fstab")
    self.assertEqual(self._cleanupCalled, 1)

  def test_symlink(self):
    tempDir = tempfile.mkdtemp(prefix="test_cronbackoff.")
    stateDir = os.path.join(tempDir, "sym")
    os.symlink(".", stateDir)
    cronbackoff.mkStateDir(stateDir)
    self.assertEqual(self._cleanupCalled, 1)
    os.unlink(stateDir)
    os.rmdir(tempDir)
