"""Exercise TiDB startup gating without building or starting a database."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).resolve().with_name("run-tests.sh")


class IntegrationStartupTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        (self.root / "mocks").mkdir()
        common = '''
import json, os, sys, time
from pathlib import Path
root = Path(os.environ['TEST_ROOT'])
def record(event):
    with (root / 'events').open('a') as out:
        out.write(json.dumps(event) + '\\n')
'''
        self.executable("server", common + '''
record({'kind': 'server', 'pid': os.getpid(), 'args': sys.argv[1:]})
print('TiDB bootstrap diagnostic', flush=True)
if os.environ['SCENARIO'] == 'exit':
    sys.exit(7)
time.sleep(60)
''')
        self.executable("mysql_tester", common + '''
assert (root / 'ready').exists(), 'test runner started before TiDB was ready'
(root / 'ready').unlink()
record({'kind': 'test', 'args': sys.argv[1:]})
sys.exit(int(os.environ.get('TEST_EXIT_CODE', '0')))
''')
        self.executable("mocks/curl", common + '''
assert '--noproxy' in sys.argv and '--max-time' in sys.argv
assert sys.argv[-1] == 'http://127.0.0.1:4001/status'
events = [json.loads(line) for line in (root / 'events').read_text().splitlines()] if (root / 'events').exists() else []
record({'kind': 'probe'})
if not any(e['kind'] == 'server' for e in events):
    sys.exit(1)
scenario = os.environ['SCENARIO']
if scenario in ('exit', 'timeout'):
    sys.exit(1)
if scenario == 'delayed' and sum(e['kind'] == 'probe' for e in events) < 2:
    sys.exit(1)
(root / 'ready').touch()
''')
        self.executable("mocks/lsof", "import sys\nsys.exit(1)\n")
        self.executable("mocks/unzip", "")
        # Keep readiness retries fast; Bash's SECONDS still bounds the timeout.
        self.executable("mocks/sleep", "import time\ntime.sleep(0.05)\n")

    def executable(self, name, body):
        path = self.root / name
        path.write_text("#!{}\n{}".format(sys.executable, body))
        path.chmod(0o755)

    def events(self):
        path = self.root / "events"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def run_runner(self, collation="y", scenario="", timeout="5", exit_code="0"):
        env = dict(os.environ, TEST_ROOT=str(self.root), SCENARIO=scenario,
                   TIDB_TEST_STORE_NAME="unistore", TIKV_PATH="",
                   TIDB_TEST_STARTUP_TIMEOUT_SECONDS=timeout,
                   TEST_EXIT_CODE=exit_code,
                   PATH=str(self.root / "mocks") + os.pathsep + os.environ["PATH"])
        process = subprocess.Popen(
            ["bash", str(RUNNER), "-b", "n", "-s", "./server", "-d", collation,
             "-t", "collation_startup"], cwd=str(self.root), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True, start_new_session=True)
        try:
            output, _ = process.communicate(timeout=12)
            return process.returncode, output
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            # Python 3.6, still used by CI images, selects on the already closed
            # pipes when communicate() is called twice and raises ValueError.
            # Reap and close here instead so a finished process is still cleaned.
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            if process.stdout is not None:
                process.stdout.close()

    def test_both_collation_modes_wait_for_readiness(self):
        code, output = self.run_runner(collation="b", scenario="delayed")
        self.assertEqual(code, 0, output)
        servers = [e for e in self.events() if e["kind"] == "server"]
        self.assertEqual(len(servers), 2)
        self.assertIn("disable_new_collation.toml", servers[0]["args"])
        self.assertIn("config.toml", servers[1]["args"])
        tests = [e for e in self.events() if e["kind"] == "test"]
        self.assertEqual(len(tests), 2)
        self.assertIn("--collation-disable=true", tests[0]["args"])
        self.assertIn("--collation-disable=false", tests[1]["args"])
        self.assertGreaterEqual(sum(e["kind"] == "probe" for e in self.events()), 3)

    def test_early_exit_prints_log_without_running_tests(self):
        code, output = self.run_runner(scenario="exit")
        self.assertNotEqual(code, 0, output)
        self.assertIn("exited before becoming ready", output)
        self.assertIn("TiDB bootstrap diagnostic", output)
        self.assertFalse(any(e["kind"] == "test" for e in self.events()))

    def test_startup_timeout_prints_log_without_running_tests(self):
        code, output = self.run_runner(scenario="timeout", timeout="1")
        self.assertNotEqual(code, 0, output)
        self.assertIn("Timed out after 1s", output)
        self.assertIn("TiDB bootstrap diagnostic", output)
        self.assertFalse(any(e["kind"] == "test" for e in self.events()))

    def test_sql_test_failure_is_preserved(self):
        code, output = self.run_runner(exit_code="43")
        self.assertEqual(code, 43, output)

    def test_invalid_timeout_starts_nothing(self):
        code, output = self.run_runner(timeout="0")
        self.assertEqual(code, 2, output)
        self.assertIn("positive integer", output)
        self.assertEqual(self.events(), [])


if __name__ == "__main__":
    unittest.main()
