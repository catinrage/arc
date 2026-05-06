import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SERVICE = ROOT / "service.sh"


class ServiceScriptTests(unittest.TestCase):
    def test_service_script_syntax(self):
        result = subprocess.run(["bash", "-n", str(SERVICE)], cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_usage_without_args(self):
        result = subprocess.run([str(SERVICE)], cwd=ROOT, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("./service.sh update", result.stdout)

    def test_status_without_initialized_services(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_systemctl = Path(tmp) / "systemctl"
            fake_systemctl.write_text(
                "#!/usr/bin/env bash\n"
                "case \"$1\" in\n"
                "  is-active) echo inactive ;;\n"
                "  is-enabled) echo disabled ;;\n"
                "  *) exit 0 ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            fake_systemctl.chmod(fake_systemctl.stat().st_mode | stat.S_IXUSR)

            env = os.environ.copy()
            env["SYSTEMCTL_BIN"] = str(fake_systemctl)
            result = subprocess.run(
                [str(SERVICE), "status"],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("agent", result.stdout)
        self.assertIn("gateway", result.stdout)
        self.assertIn("not-initialized", result.stdout)


if __name__ == "__main__":
    unittest.main()
