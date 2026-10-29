#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyyaml>=6.0",
# ]
# ///
"""Fill lcls_lclsstream_nersc_template.sh with parameters from a YAML file
and run the resulting script.

Every {{param}} placeholder found in the template must have a matching key
in the YAML file; if any are missing, this raises an error and refuses to
run anything.

    uv run run_lclsstream_nersc_template.py params.yaml
    uv run run_lclsstream_nersc_template.py params.yaml --template other.sh
    uv run run_lclsstream_nersc_template.py params.yaml --dry-run
"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

DEFAULT_TEMPLATE = Path(__file__).parent / "lcls_lclsstream_nersc_template.sh"

PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def load_params(yaml_path: Path) -> dict:
    with yaml_path.open() as f:
        params = yaml.safe_load(f) or {}
    if not isinstance(params, dict):
        raise ValueError(
            f"{yaml_path} must contain a mapping of parameter names to values"
        )
    return params


def find_placeholders(template: str) -> set[str]:
    return set(PLACEHOLDER_RE.findall(template))


def render(template: str, params: dict) -> str:
    placeholders = find_placeholders(template)
    missing = placeholders - params.keys()
    if missing:
        raise ValueError(
            "Missing values for template parameter(s): " + ", ".join(sorted(missing))
        )

    def substitute(match: re.Match) -> str:
        return str(params[match.group(1)])

    return PLACEHOLDER_RE.sub(substitute, template)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("params", type=Path, help="YAML file with template parameters")
    parser.add_argument(
        "--template",
        type=Path,
        default=DEFAULT_TEMPLATE,
        help=f"template script to fill (default: {DEFAULT_TEMPLATE.name})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the rendered script instead of running it",
    )
    args = parser.parse_args()

    template_text = args.template.read_text()
    params = load_params(args.params)
    rendered = render(template_text, params)

    if args.dry_run:
        print(rendered)
        return 0

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sh", prefix="lclsstream_", delete=False
    ) as tmp:
        tmp.write(rendered)
        tmp_path = Path(tmp.name)
    tmp_path.chmod(0o700)

    try:
        result = subprocess.run(["/bin/bash", str(tmp_path)])
        return result.returncode
    finally:
        tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
