#!/usr/bin/env python3
"""Validate zeroeye diagnostic build artifacts.

The tool checks diagnostic JSON structure, verifies that referenced log artifacts
exist, and can run lightweight subprocess checks with recoverable error output.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIAGNOSTIC_DIR = ROOT / "diagnostic"


@dataclass
class VerificationResult:
    ok: bool
    checked: int
    passing_modules: int
    errors: list[str]
    warnings: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "checked": self.checked,
            "passing_modules": self.passing_modules,
            "errors": self.errors,
            "warnings": self.warnings,
        }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate zeroeye diagnostic artifacts")
    parser.add_argument(
        "diagnostic",
        nargs="?",
        default=str(DEFAULT_DIAGNOSTIC_DIR),
        help="Diagnostic JSON file or directory to inspect (default: diagnostic/)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print validation details")
    parser.add_argument("--json", action="store_true", dest="json_output", help="Emit machine-readable JSON")
    parser.add_argument(
        "--threshold",
        type=int,
        default=0,
        metavar="N",
        help="Minimum number of passing modules required (default: 0)",
    )
    args = parser.parse_args(argv)
    if args.threshold < 0:
        parser.error("--threshold must be zero or a positive integer")
    return args


def run_command(command: list[str], cwd: Path = ROOT) -> tuple[bool, str]:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except FileNotFoundError as exc:
        return False, f"command not found: {command[0]} ({exc})"
    except subprocess.TimeoutExpired as exc:
        return False, f"command timed out after {exc.timeout}s: {' '.join(command)}"
    except OSError as exc:
        return False, f"failed to run {' '.join(command)}: {exc}"

    output = (completed.stdout or completed.stderr or "").strip()
    if completed.returncode != 0:
        return False, f"command exited {completed.returncode}: {' '.join(command)}; {output}"
    return True, output


def diagnostic_json_files(target: Path) -> list[Path]:
    if target.is_file():
        if target.suffix != ".json":
            raise ValueError(f"expected a .json diagnostic file, got {target}")
        return [target]
    if not target.exists():
        raise ValueError(f"diagnostic path does not exist: {target}")
    if not target.is_dir():
        raise ValueError(f"diagnostic path is neither file nor directory: {target}")
    files = sorted(target.glob("build-*.json")) + sorted(target.glob("build-*-metadata.json"))
    unique: dict[Path, None] = {path: None for path in files}
    return list(unique)


def require_type(data: dict[str, Any], key: str, expected: type, errors: list[str], path: Path) -> None:
    if key not in data:
        errors.append(f"{path}: missing required key {key!r}")
    elif not isinstance(data[key], expected):
        errors.append(f"{path}: key {key!r} must be {expected.__name__}, got {type(data[key]).__name__}")


def validate_schema(path: Path) -> tuple[int, list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return 0, [f"{path}: invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"], []
    except OSError as exc:
        return 0, [f"{path}: cannot read file: {exc}"], []

    if not isinstance(data, dict):
        return 0, [f"{path}: diagnostic JSON root must be an object"], []

    module_count = 0
    if "modules" in data:
        if not isinstance(data["modules"], list):
            errors.append(f"{path}: 'modules' must be a list")
        else:
            module_count = len(data["modules"])
            for index, module in enumerate(data["modules"]):
                if not isinstance(module, dict):
                    errors.append(f"{path}: modules[{index}] must be an object")
                    continue
                require_type(module, "name", str, errors, path)
                if "success" in module and not isinstance(module["success"], bool):
                    errors.append(f"{path}: modules[{index}].success must be bool")
                if "duration" in module and not isinstance(module["duration"], (int, float)):
                    errors.append(f"{path}: modules[{index}].duration must be numeric")
    elif "results" in data:
        if not isinstance(data["results"], list):
            errors.append(f"{path}: 'results' must be a list")
        else:
            module_count = len(data["results"])
    else:
        warnings.append(f"{path}: no modules/results array present; structural validation was limited")

    artifact_keys = ["diagnostic_logd", "logd", "logd_path", "artifacts"]
    for key in artifact_keys:
        if key not in data:
            continue
        value = data[key]
        values = value if isinstance(value, list) else [value]
        for item in values:
            if not isinstance(item, str):
                errors.append(f"{path}: {key} entries must be strings")
                continue
            artifact = (path.parent / item).resolve() if not Path(item).is_absolute() else Path(item)
            if not artifact.exists():
                errors.append(f"{path}: referenced artifact does not exist: {item}")
    return module_count, errors, warnings


def verify(target: Path, threshold: int, verbose: bool) -> VerificationResult:
    errors: list[str] = []
    warnings: list[str] = []
    passing_modules = 0
    checked = 0

    ok, message = run_command([sys.executable, "--version"])
    if not ok:
        errors.append(message)
    elif verbose:
        warnings.append(f"python version check: {message}")

    try:
        files = diagnostic_json_files(target)
    except ValueError as exc:
        return VerificationResult(False, 0, 0, [str(exc)], warnings)

    if not files:
        errors.append(f"no diagnostic JSON files found in {target}")

    for file_path in files:
        module_count, file_errors, file_warnings = validate_schema(file_path)
        checked += 1
        passing_modules += module_count
        errors.extend(file_errors)
        warnings.extend(file_warnings)

    if passing_modules < threshold:
        errors.append(
            f"passing module threshold not met: required {threshold}, found {passing_modules}"
        )

    return VerificationResult(not errors, checked, passing_modules, errors, warnings)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    result = verify(Path(args.diagnostic), args.threshold, args.verbose)

    if args.json_output:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        status = "ok" if result.ok else "failed"
        print(f"diagnostic verification {status}: checked={result.checked} passing_modules={result.passing_modules}")
        for warning in result.warnings:
            if args.verbose:
                print(f"warning: {warning}", file=sys.stderr)
        for error in result.errors:
            print(f"error: {error}", file=sys.stderr)

    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
