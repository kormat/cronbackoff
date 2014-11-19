"""
Test suite for cronbackoff.py

To be run through nose (https://nose.readthedocs.org/), not directly
"""

import errno
import logging
import os
import sys
import tempfile
import time
import unittest

import cronbackoff

# TODO(kormat): run all tests with and without debug enabled.


class TestParseArgs(unittest.TestCase):
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
    opts = cronbackoff._parseArgs(args)

    # No exceptions raised, good start.
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
    opts = cronbackoff._parseArgs(
        [prog, command])

    # No exceptions raised, good start.
    self.assertEqual(opts.base_delay, 60)
    self.assertEqual(opts.max_delay, 1440)
    self.assertAlmostEqual(opts.exponent, 4)
    self.assertEqual(opts.debug, False)
    self.assertEqual(opts.name, name)
    self.assertEqual(opts.command, [command])

class TestFormatTime(unittest.TestCase):
  def test_zero(self):
    self.assertEqual(cronbackoff._formatTime(0), "0s")

  def test_1s(self):
    self.assertEqual(cronbackoff._formatTime(1), "1s")

  def test_1m(self):
    self.assertEqual(cronbackoff._formatTime(60), "1m")

  def test_1h(self):
    self.assertEqual(cronbackoff._formatTime(3600), "1h")

  def test_mins_seconds(self):
    self.assertEqual(cronbackoff._formatTime(82), "1m 22s")

  def test_hours_mins_seconds(self):
    self.assertEqual(cronbackoff._formatTime(3782), "1h 3m 2s")

  def test_precision_seconds(self):
    self.assertEqual(cronbackoff._formatTime(10799, precision="seconds"), "2h 59m 59s")

  def test_precision_minutes(self):
    self.assertEqual(cronbackoff._formatTime(10799, precision="minutes"), "2h 59m")

  def test_precision_hours(self):
    self.assertEqual(cronbackoff._formatTime(10799, precision="hours"), "2h")

  def test_precision_seconds_zero(self):
    self.assertEqual(cronbackoff._formatTime(0, precision="seconds"), "0s")

  def test_precision_minutes_zero(self):
    self.assertEqual(cronbackoff._formatTime(0, precision="minutes"), "0m")

  def test_precision_hours_zero(self):
    self.assertEqual(cronbackoff._formatTime(0, precision="hours"), "0h")

class TestExecute(unittest.TestCase):
  def setUp(self):
    super(TestExecute, self).setUp()
    self.tempDir = tempfile.mkdtemp(prefix=self.id())

  def tearDown(self):
    super(TestExecute, self).tearDown()
    os.rmdir(self.tempDir)
    del self.tempDir

  def test_success(self):
    testScript = os.path.join(self.tempDir, "test")
    with open(testScript, "w") as f:
      f.write("#!/bin/bash\n\necho TESTING\nexit 0")
      os.fchmod(f.fileno(), 0o700)
    self.assertTrue(cronbackoff.execute([testScript]))
    os.unlink(testScript)

  def test_failure(self):
    testScript = os.path.join(self.tempDir, "test")
    with open(testScript, "w") as f:
      f.write("#!/bin/bash\n\necho TESTING\nexit 1")
      os.fchmod(f.fileno(), 0o700)
    self.assertFalse(cronbackoff.execute([testScript]))
    os.unlink(testScript)

  def test_not_found(self):
    testScript = os.path.join(self.tempDir, "test")
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      cronbackoff.execute([testScript])
    self.assertEqual(ctx.exception.errno, errno.ENOENT)

  def test_not_executable(self):
    testScript = os.path.join(self.tempDir, "test")
    open(testScript, 'w').close()
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      cronbackoff.execute([testScript])
    self.assertEqual(ctx.exception.errno, errno.EACCES)
    os.unlink(testScript)

class StateWrapper(unittest.TestCase):
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

class TestStateMkDir(StateWrapper):
  def test_dir_exists(self):
    self.state._mkDir()
    # No exceptions raised, all is good.

  def test_new_dir(self):
    self.state.dir = os.path.join(self.tempDir, "subdir")
    self.state._mkDir()
    self.assertTrue(os.path.isdir(self.state.dir))
    # No exceptions raised, all is good.
    os.rmdir(self.state.dir)

  def test_no_mkdir_perms(self):
    self.state.dir = os.path.join(self.tempDir, "subdir")
    os.chmod(self.tempDir, 0o500)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._mkDir()
    self.assertEqual(ctx.exception.errno, errno.EACCES)

  def test_dir_wrong_owner(self):
    # It's non-trivial to create dir with wrong owner, so just use one that's guaranteed to exist.
    self.state.dir="/"
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._mkDir()
    self.assertTrue("not owned by" in ctx.exception.message)

  def test_file(self):
    self.state.dir="/etc/fstab"
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._mkDir()
    self.assertTrue("not a dir" in ctx.exception.message)

  def test_symlink(self):
    self.state.dir = os.path.join(self.tempDir, "sym")
    os.symlink(".", self.state.dir)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._mkDir()
    self.assertTrue("symlink" in ctx.exception.message)
    os.unlink(self.state.dir)

class TestStateLock(StateWrapper):
  def test_state(self):
    open(self.state.filePath, 'w').close()
    self.state._lock()
    self.assertTrue(self.state.stateExists)
    self.state.file.close()
    os.unlink(self.state.filePath)

  def test_no_state(self):
    self.state._lock()
    self.assertFalse(self.state.stateExists)
    self.assertTrue(os.path.isfile(self.state.filePath))
    self.state.file.close()
    os.unlink(self.state.filePath)

  def test_no_state_dir(self):
    self.state.dir = os.path.join(self.tempDir, "noexisty")
    self.state.filePath = os.path.join(self.state.dir, self.state.name)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._lock()
    # Actually comes from trying to create a new state file:
    self.assertEqual(ctx.exception.errno, errno.ENOENT)

  def test_no_dir_read_perms(self):
    # Technically, dir doesn't have the execute bit set :P
    os.chmod(self.tempDir, 0o600)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._lock()
    self.assertEqual(ctx.exception.errno, errno.EACCES)

  def test_no_dir_write_perms(self):
    os.chmod(self.tempDir, 0o500)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._lock()
    self.assertEqual(ctx.exception.errno, errno.EACCES)

  def test_no_file_read_perms(self):
    with open(self.state.filePath, 'w') as f:
      os.fchmod(f.fileno(), 0o200)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._lock()
    self.assertEqual(ctx.exception.errno, errno.EACCES)
    os.unlink(self.state.filePath)

  def test_no_file_write_perms(self):
    with open(self.state.filePath, 'w') as f:
      os.fchmod(f.fileno(), 0o400)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._lock()
    self.assertEqual(ctx.exception.errno, errno.EACCES)
    os.unlink(self.state.filePath)

  def test_file_locked(self):
    self.state._lock()
    newstate = cronbackoff.State(self.tempDir, self.name)
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      newstate._lock()
    self.assertEqual(ctx.exception.errno, errno.EAGAIN)
    os.unlink(self.state.filePath)

class TestStateRead(StateWrapper):
  def test_no_state(self):
    self.state._lock()
    self.state._read()
    # No exceptions raised, good start.
    self.assertIsNone(self.state.lastRun)
    os.unlink(self.state.filePath)

  def test_has_state(self):
    with open(self.state.filePath, 'w') as f:
      f.write("12\n")
    now = time.time()
    os.utime(self.state.filePath, (now, now))
    self.state._lock()
    self.state._read()
    # Using AlmostEqual as FS timestamp precision can cause minor differences
    self.assertAlmostEqual(self.state.lastRun, now, delta=1)
    self.assertAlmostEqual(self.state.nextRun, now+12*60, delta=1)
    self.assertEqual(self.state.lastDelay, 12)
    os.unlink(self.state.filePath)

  def test_has_state_zero(self):
    with open(self.state.filePath, 'w') as f:
      f.write("0\n")
    now = time.time()
    os.utime(self.state.filePath, (now, now))
    self.state._lock()
    self.state._read()
    # Using AlmostEqual as FS timestamp precision can cause minor differences
    self.assertAlmostEqual(self.state.lastRun, now, delta=1)
    self.assertAlmostEqual(self.state.nextRun, now, delta=1)
    self.assertEqual(self.state.lastDelay, 0)
    os.unlink(self.state.filePath)

  def test_empty_state(self):
    open(self.state.filePath, 'w').close()
    self.state._lock()
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._read()
    self.assertTrue("not a valid" in ctx.exception.message)
    os.unlink(self.state.filePath)

  def test_invalid_state(self):
    with open(self.state.filePath, 'w') as f:
      f.write("Hello, world")
    self.state._lock()
    with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
      self.state._read()
    self.assertTrue("not a valid" in ctx.exception.message)
    os.unlink(self.state.filePath)

  def test_read_error(self):
    with open(self.state.filePath, 'w') as self.state.file:
      with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
        self.state._read()
    self.assertTrue("not open for" in ctx.exception.message)
    os.unlink(self.state.filePath)

class TestStateBackoff(StateWrapper):
  def test_no_state(self):
    self.state.stateExists = False
    self.assertIsNone(self.state._backoff())

  def test_no_delay(self):
    self.state.lastDelay = 0
    self.assertEqual(self.state._backoff(), 0)

  def test_in_backoff(self):
    self.state.nextRun = time.time() + 10
    self.assertAlmostEqual(self.state._backoff(), 10, delta=0.1)

  def test_out_of_backoff(self):
    self.state.lastDelay = 45
    self.state.nextRun = time.time() - 10
    self.state._backoff()

class TestStateSave(StateWrapper):
  def setUp(self):
    super(TestStateSave, self).setUp()
    self.state._lock()
    self.state._read()

  def tearDown(self):
    os.unlink(self.state.filePath)
    super(TestStateSave, self).tearDown()

  def _basic_test(self, contents, args):
    self.state.save(*args)

    with open(self.state.filePath) as f:
      self.assertEqual(f.read(), contents)

  def test_mtime(self):
    self.state.save(True, 1, 1, 1)

    st = os.lstat(self.state.filePath)
    self.assertAlmostEqual(st.st_mtime, time.time(), delta=1)

  def test_success(self):
    self._basic_test("0\n", (True, 1, 1, 1))

  def test_no_state(self):
    self.state.lastDelay = None
    self._basic_test("133\n", (False, 133, 300, 3))

  def test_no_state_max(self):
    self.state.lastDelay = None
    self._basic_test("98\n", (False, 99, 98, 7))

  def test_no_lastDelay(self):
    self.state.lastDelay = 0
    self._basic_test("133\n", (False, 133, 300, 3))

  def test_no_lastDelay_max(self):
    self.state.lastDelay = 0
    self._basic_test("98\n", (False, 99, 98, 7))

  def test_lastDelay(self):
    self.state.lastDelay = 33
    self._basic_test("330\n", (False, 12, 999, 10))

  def test_lastDelay_max(self):
    self.state.lastDelay = 33
    self._basic_test("263\n", (False, 12, 263, 10))

  def test_write_error(self):
    self.state.file.close()
    with open(self.state.filePath, 'r') as self.state.file:
      with self.assertRaises(cronbackoff.CronBackoffException) as ctx:
        self.state.save(False, 1, 1, 1)
    self.assertTrue("not open for" in ctx.exception.message)
