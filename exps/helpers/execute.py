#!/usr/bin/env python3
"""Execute a set of NIS runs."""

import json
import random
import subprocess
import sys
from pathlib import Path


class _Tee:
    """Write to both the original stdout and a log file."""
    def __init__(self, log_path):
        self._stdout = sys.stdout
        self._log = open(log_path, "a", buffering=1)

    def write(self, data):
        self._stdout.write(data)
        self._log.write(data)

    def flush(self):
        self._stdout.flush()
        self._log.flush()

    def close(self):
        self._log.close()

    # Proxy anything else (e.g. fileno, isatty) to the real stdout
    def __getattr__(self, name):
        return getattr(self._stdout, name)


def _run(cmd, label):
    """Run a subprocess, streaming output to stdout (and optionally a log file)."""
    proc = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in proc.stdout:
        sys.stdout.write(line)
    proc.wait()
    return proc.returncode


def execute_runs(runs, nis_path, log_path=None):
    tee = None
    if log_path:
        tee = _Tee(log_path)
        sys.stdout = tee

    try:
        for run_name, run in runs.items():

            if run.get("skip", False): continue

            print(f"\n{'='*60}")
            print(f"RUN: {run_name}")
            print(f"{'='*60}")

            data = run["data"]
            mode = run["mode"]
            work = run.get("work", [])

            # ── optional pre-script ───────────────────────────────────────────────
            pre_script = run.get("pre_script")
            if pre_script:
                script_file = str(pre_script["file"])
                script_args = [str(a) for a in pre_script.get("args", [])]
                pre_cmd = ["python3", script_file] + script_args
                print("Pre-script:", " ".join(pre_cmd))
                rc = _run(pre_cmd, "pre_script")
                if rc != 0:
                    raise RuntimeError(f"  [PRE-SCRIPT FAILED] exit code {rc}")

            if mode == "query":
                configs = []
                for cfg_entry in run["configs"]:
                    cfg_file = cfg_entry["file"]
                    limit    = cfg_entry.get("limit", None)
                    items = json.loads(Path(cfg_file).read_text())
                    lines = [json.dumps(item, separators=(',', ':')) for item in items.values()]
                    if limit is not None:
                        random.shuffle(lines)
                        lines = lines[:limit]
                    configs.extend(lines)

                config_str = "|".join(configs)

                if len(work) == 0:
                    cmd = [str(nis_path), mode, config_str, str(data)]
                else:
                    qry_str = "|".join(str(q) for q in work)
                    cmd = [str(nis_path), mode, config_str, str(data), qry_str]

                print("Running:", " ".join(cmd))
                rc = _run(cmd, "query")
                if rc != 0:
                    raise RuntimeError(f"  [FAILED] exit code {rc}")

            else:
                for cfg_entry in run["configs"]:
                    cfg_file = cfg_entry["file"]
                    limit    = cfg_entry.get("limit", None)
                    items = json.loads(Path(cfg_file).read_text())

                    for key, item in items.items():
                        line = json.dumps(item, separators=(',', ':'))
                        for w in work:
                            cmd = [str(nis_path), mode, line, str(data), str(w)]
                            print(f"Running {key}:", " ".join(cmd))
                            rc = _run(cmd, mode)
                            if rc != 0:
                                raise RuntimeError(f"  [FAILED] exit code {rc}")

    finally:
        if tee:
            sys.stdout = tee._stdout
            tee.close()
