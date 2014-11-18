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


class CleanupExitException(Exception):
  pass


class CleanupExitMocker(unittest.TestCase):
  def setUp(self):
    self._orig_cleanupExit = cronbackoff.cleanupExit
    cronbackoff.cleanupExit = self._mock_cleanupExit

  def tearDown(self):
    cronbackoff.cleanupExit = self._orig_cleanupExit

  def _mock_cleanupExit(self, state):
    if state != 0:
      raise CleanupExitException


class TestParseArgs(CleanupExitMocker):
  def test_basic(self):
    prog = "nosetests"
    base_delay = 10
    max_delay = 10000
    exponent = 22.1222332312
    name = self.id()
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

    # CleanupExitException not raised, good start.
    self.assertEqual(opts.base_delay, base_delay)
    self.assertEqual(opts.max_delay, max_delay)
    self.assertAlmostEqual(opts.exponent, exponent)
    self.assertEqual(opts.debug, True)
    self.assertEqual(opts.name, name)
    self.assertEqual(opts.command, command)

  def test_defaults(self):
    prog = "nosetests"
    name = self.id()
    command = "/usr/bin/%s" % name
    opts = cronbackoff.parseArgs(
        [prog, command])

    # CleanupExitException not raised, good start.
    self.assertEqual(opts.base_delay, 60)
    self.assertEqual(opts.max_delay, 1440)
    self.assertAlmostEqual(opts.exponent, 4)
    self.assertEqual(opts.debug, False)
    self.assertEqual(opts.name, name)
    self.assertEqual(opts.command, [command])


class TestMkStateDir(CleanupExitMocker):
  def test_dir_exists(self):
    tempDir = tempfile.mkdtemp(prefix=self.id())
    cronbackoff.mkStateDir(tempDir)
    # CleanupExitException wasn't raised, all is good.
    os.rmdir(tempDir)

  def test_new_dir(self):
    tempDir = tempfile.mkdtemp(prefix=self.id())
    stateDir = os.path.join(tempDir, "subdir")
    cronbackoff.mkStateDir(stateDir)
    self.assertTrue(os.path.isdir(stateDir))
    # CleanupExitException wasn't raised, all is good.
    os.rmdir(stateDir)
    os.rmdir(tempDir)

  def test_dir_wrong_owner(self):
    # It's non-trivial to create dir with wrong owner, so just use one that's guaranteed to exist.
    with self.assertRaises(CleanupExitException):
      cronbackoff.mkStateDir("/")

  def test_file(self):
    with self.assertRaises(CleanupExitException):
      cronbackoff.mkStateDir("/etc/fstab")

  def test_symlink(self):
    tempDir = tempfile.mkdtemp(prefix=self.id())
    stateDir = os.path.join(tempDir, "sym")
    os.symlink(".", stateDir)
    with self.assertRaises(CleanupExitException):
      cronbackoff.mkStateDir(stateDir)
    os.unlink(stateDir)
    os.rmdir(tempDir)

class TestGetLock(CleanupExitMocker):
  def setUp(self):
    super(TestGetLock, self).setUp()
    self.tempDir = tempfile.mkdtemp(prefix=self.id())

  def tearDown(self):
    super(TestGetLock, self).tearDown()
    os.rmdir(self.tempDir)
    del self.tempDir

  def test_state(self):
    name = self.id()
    stateFile = os.path.join(self.tempDir, name)
    f = open(stateFile, 'w')
    f.close()
    cronbackoff.getLock(self.tempDir, name)
    self.assertTrue(cronbackoff.stateExists)
    os.unlink(stateFile)

  def test_no_state(self):
    name = self.id()
    stateFile = os.path.join(self.tempDir, name)
    cronbackoff.getLock(self.tempDir, name)
    self.assertFalse(cronbackoff.stateExists)
    self.assertTrue(os.path.isfile(stateFile))
    os.unlink(stateFile)

  def test_no_state_dir(self):
    stateDir = os.path.join(self.tempDir, "noexisty")
    with self.assertRaises(CleanupExitException):
      cronbackoff.getLock(stateDir, self.id())

  def test_no_dir_read_perms(self):
    # Technically, dir doesn't have the execute bit set :P
    os.chmod(self.tempDir, 0o600)
    with self.assertRaises(CleanupExitException):
      cronbackoff.getLock(self.tempDir, self.id())

  def test_no_dir_write_perms(self):
    os.chmod(self.tempDir, 0o500)
    with self.assertRaises(CleanupExitException):
      cronbackoff.getLock(self.tempDir, self.id())

  def test_no_file_read_perms(self):
    name = self.id()
    stateFile = os.path.join(self.tempDir, name)
    f = open(stateFile, 'w')
    os.fchmod(f.fileno(), 0o200)
    f.close()
    with self.assertRaises(CleanupExitException):
      cronbackoff.getLock(self.tempDir, name)
    os.unlink(stateFile)

  def test_no_file_write_perms(self):
    name = self.id()
    stateFile = os.path.join(self.tempDir, name)
    f = open(stateFile, 'w')
    os.fchmod(f.fileno(), 0o400)
    f.close()
    with self.assertRaises(CleanupExitException):
      cronbackoff.getLock(self.tempDir, name)
    os.unlink(stateFile)

  def test_file_locked(self):
    name = self.id()
    stateFile = os.path.join(self.tempDir, name)
    cronbackoff.getLock(self.tempDir, name)
    with self.assertRaises(CleanupExitException):
      cronbackoff.getLock(self.tempDir, name)
    os.unlink(stateFile)
