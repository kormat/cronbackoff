"""
Test suite for cronbackoff.py

To be run through nose (https://nose.readthedocs.org/), not directly
"""

import logging
import os
import sys
import tempfile
import time
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

class TestFormatTime(unittest.TestCase):
  def test_zero(self):
    self.assertEqual(cronbackoff.formatTime(0), "0s")

  def test_1s(self):
    self.assertEqual(cronbackoff.formatTime(1), "1s")

  def test_1m(self):
    self.assertEqual(cronbackoff.formatTime(60), "1m")

  def test_1h(self):
    self.assertEqual(cronbackoff.formatTime(3600), "1h")

  def test_mins_seconds(self):
    self.assertEqual(cronbackoff.formatTime(82), "1m 22s")

  def test_hours_mins_seconds(self):
    self.assertEqual(cronbackoff.formatTime(3782), "1h 3m 2s")

  def test_precision_seconds(self):
    self.assertEqual(cronbackoff.formatTime(10799, precision="seconds"), "2h 59m 59s")

  def test_precision_minutes(self):
    self.assertEqual(cronbackoff.formatTime(10799, precision="minutes"), "2h 59m")

  def test_precision_hours(self):
    self.assertEqual(cronbackoff.formatTime(10799, precision="hours"), "2h")

  def test_precision_seconds_zero(self):
    self.assertEqual(cronbackoff.formatTime(0, precision="seconds"), "0s")

  def test_precision_minutes_zero(self):
    self.assertEqual(cronbackoff.formatTime(0, precision="minutes"), "0m")

  def test_precision_hours_zero(self):
    self.assertEqual(cronbackoff.formatTime(0, precision="hours"), "0h")

class StateWrapper(CleanupExitMocker):
  def setUp(self):
    super(StateWrapper, self).setUp()
    self.tempDir = tempfile.mkdtemp(prefix=self.id())
    self.name = self.id()
    self.state = cronbackoff.State(self.tempDir, self.name)

  def tearDown(self):
    super(StateWrapper, self).tearDown()
    os.rmdir(self.tempDir)
    del self.tempDir
    del self.name
    del self.state

class TestStateMkStateDir(StateWrapper):
  def test_dir_exists(self):
    self.state.mkStateDir()
    # CleanupExitException wasn't raised, all is good.

  def test_new_dir(self):
    self.state.dir = os.path.join(self.tempDir, "subdir")
    self.state.mkStateDir()
    self.assertTrue(os.path.isdir(self.state.dir))
    # CleanupExitException wasn't raised, all is good.
    os.rmdir(self.state.dir)

  def test_no_mkdir_perms(self):
    self.state.dir = os.path.join(self.tempDir, "subdir")
    os.chmod(self.tempDir, 0o500)
    with self.assertRaises(CleanupExitException):
      self.state.mkStateDir()

  def test_dir_wrong_owner(self):
    # It's non-trivial to create dir with wrong owner, so just use one that's guaranteed to exist.
    self.state.dir="/"
    with self.assertRaises(CleanupExitException):
      self.state.mkStateDir()

  def test_file(self):
    self.state.dir="/etc/fstab"
    with self.assertRaises(CleanupExitException):
      self.state.mkStateDir()

  def test_symlink(self):
    self.state.dir = os.path.join(self.tempDir, "sym")
    os.symlink(".", self.state.dir)
    with self.assertRaises(CleanupExitException):
      self.state.mkStateDir()
    os.unlink(self.state.dir)

class TestStateGetLock(StateWrapper):
  def test_state(self):
    f = open(self.state.filePath, 'w')
    f.close()
    self.state.getLock()
    self.assertTrue(self.state.stateExists)
    self.state.file.close()
    os.unlink(self.state.filePath)

  def test_no_state(self):
    self.state.getLock()
    self.assertFalse(self.state.stateExists)
    self.assertTrue(os.path.isfile(self.state.filePath))
    self.state.file.close()
    os.unlink(self.state.filePath)

  def test_no_state_dir(self):
    self.state.dir = os.path.join(self.tempDir, "noexisty")
    self.state.filePath = os.path.join(self.state.dir, self.state.name)
    with self.assertRaises(CleanupExitException):
      self.state.getLock()

  def test_no_dir_read_perms(self):
    # Technically, dir doesn't have the execute bit set :P
    os.chmod(self.tempDir, 0o600)
    with self.assertRaises(CleanupExitException):
      self.state.getLock()

  def test_no_dir_write_perms(self):
    os.chmod(self.tempDir, 0o500)
    with self.assertRaises(CleanupExitException):
      self.state.getLock()

  def test_no_file_read_perms(self):
    f = open(self.state.filePath, 'w')
    os.fchmod(f.fileno(), 0o200)
    f.close()
    with self.assertRaises(CleanupExitException):
      self.state.getLock()
    os.unlink(self.state.filePath)

  def test_no_file_write_perms(self):
    f = open(self.state.filePath, 'w')
    os.fchmod(f.fileno(), 0o400)
    f.close()
    with self.assertRaises(CleanupExitException):
      self.state.getLock()
    os.unlink(self.state.filePath)

  def test_file_locked(self):
    self.state.getLock()
    newstate = cronbackoff.State(self.tempDir, self.name)
    with self.assertRaises(CleanupExitException):
      newstate.getLock()
    os.unlink(self.state.filePath)

class TestStateRead(StateWrapper):
  def test_no_state(self):
    self.state.getLock()
    self.state.read()
    # CleanupExitException not raised, good start.
    self.assertIsNone(self.state.lastRun)
    os.unlink(self.state.filePath)

  def test_has_state(self):
    f = open(self.state.filePath, 'w')
    f.write("12\n")
    f.close()
    now = time.time()
    os.utime(self.state.filePath, (now, now))
    self.state.getLock()
    self.state.read()
    # Using AlmostEqual as FS timestamp precision can cause minor differences
    self.assertAlmostEqual(self.state.lastRun, now, delta=1)
    self.assertAlmostEqual(self.state.nextRun, now+12*60, delta=1)
    self.assertEqual(self.state.lastDelay, 12)
    os.unlink(self.state.filePath)

  def test_no_state(self):
    f = open(self.state.filePath, 'w')
    f.close()
    self.state.getLock()
    with self.assertRaises(CleanupExitException):
      self.state.read()
    os.unlink(self.state.filePath)
