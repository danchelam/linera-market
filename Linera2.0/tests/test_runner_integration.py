from __future__ import annotations

import hashlib
import http.server
import importlib.util
import json
import sys
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[2]


def load_runner():
    spec = importlib.util.spec_from_file_location("linera_runner_integration", ROOT / "linera_runner.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RunnerIntegrationTests(unittest.TestCase):
    def test_local_raw_rehearsal_migrates_and_removes_legacy_files(self):
        runner = load_runner()
        with TemporaryDirectory() as raw_name, TemporaryDirectory() as install_name:
            raw = Path(raw_name)
            install = Path(install_name)
            files = {
                "linera_runner.py": b"# new runner\n",
                "Linera2.0/linera2/runtime.py": b"RUNTIME_VERSION = 'integration'\n",
                "Linera2.0/README.md": b"# Linera2\n",
                "Linera2.0/requirements.txt": b"requests>=2.32,<3\n",
            }
            for relative, content in files.items():
                target = raw / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            manifest = {
                "schema_version": 2,
                "runner_version": "integration.1",
                "app_version": "integration.1",
                "task_version": "",
                "base_version": "",
                "files": [
                    {"path": path, "sha256": hashlib.sha256(content).hexdigest()}
                    for path, content in files.items()
                ],
                "remove": ["base_module.py", "linera_task.py", "test_full_flow.py"],
            }
            (raw / "version.json").write_text(json.dumps(manifest), encoding="utf-8")

            for name, content in {
                "base_module.py": "OKX_DEFAULT_PASSWORD = 'local-only-password'\n",
                "linera_task.py": "legacy\n",
                "test_full_flow.py": "legacy\n",
                "hubshuju.xlsx": "private account data\n",
            }.items():
                (install / name).write_text(content, encoding="utf-8")
            (install / "Linera2.0" / "auto_sessions.json").parent.mkdir(parents=True)
            (install / "Linera2.0" / "auto_sessions.json").write_text("{\"625421671\": {}}", encoding="utf-8")

            class QuietHandler(http.server.SimpleHTTPRequestHandler):
                def log_message(self, *_args):
                    pass

            handler = lambda *args, **kwargs: QuietHandler(*args, directory=str(raw), **kwargs)
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                result = runner.run_update(install, f"http://127.0.0.1:{server.server_port}")
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()

            self.assertTrue(result.updated)
            self.assertTrue(result.removals_completed)
            self.assertTrue((install / "Linera2.0/linera2/runtime.py").is_file())
            self.assertEqual(
                json.loads((install / "Linera2.0/local_config.json").read_text(encoding="utf-8")),
                {"wallet_password": "local-only-password"},
            )
            self.assertTrue((install / "hubshuju.xlsx").is_file())
            self.assertTrue((install / "Linera2.0/auto_sessions.json").is_file())
            for name in ("base_module.py", "linera_task.py", "test_full_flow.py"):
                self.assertFalse((install / name).exists())

    def test_remote_unavailable_keeps_existing_install_usable(self):
        runner = load_runner()
        with TemporaryDirectory() as install_name:
            install = Path(install_name)
            package = install / "Linera2.0/linera2"
            package.mkdir(parents=True)
            (package / "runtime.py").write_text("stable = True\n", encoding="utf-8")
            with self.assertRaises(OSError):
                runner.run_update(install, "http://127.0.0.1:1")
            self.assertTrue((package / "runtime.py").is_file())

    def test_fetch_remote_falls_back_to_second_base(self):
        runner = load_runner()
        self.assertEqual(
            runner._remote_base_urls("https://raw.githubusercontent.com/acme/repo/refs/heads/main"),
            (
                "https://raw.githubusercontent.com/acme/repo/refs/heads/main",
                "https://cdn.jsdelivr.net/gh/acme/repo@main",
            ),
        )


if __name__ == "__main__":
    unittest.main()
