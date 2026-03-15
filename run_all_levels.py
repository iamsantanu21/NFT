import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

# Level 3 script selection (fixed to full activity history)
LEVEL3_SCRIPT = "Level3v4.py"


def build_scripts() -> list[str]:
    return [
        "Level1.py",                    # Collection info
        "Level2.py",                    # NFT metadata
        LEVEL3_SCRIPT,                  # Activity data  ← controlled above
        "Level4_financial_dataset.py",  # Financial summary
    ]


def run_script(script_name: str, env: dict) -> int:
    script_path = Path(script_name)
    if not script_path.exists():
        print(f"Missing script: {script_name}")
        return 1

    print("\n" + "=" * 70)
    print(f"Running {script_name}")
    print("=" * 70)

    result = subprocess.run([sys.executable, str(script_path)], env=env)
    return result.returncode


def normalize_collection_slug(value: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object.")

    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Level1 to Level4 in sequence.")
    parser.add_argument(
        "--collection",
        required=False,
        help="Collection slug or name (example: mutant-ape-yacht-club or Mutant Ape Yacht Club).",
    )
    parser.add_argument(
        "--config",
        default="pipeline_config.json",
        help="Path to JSON config file containing collection and opensea_api_key.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force refresh and re-run all levels even if output files already exist.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"Failed to read config file {config_path}: {e}")
        return 1

    collection_value = args.collection or cfg.get("collection")
    if not collection_value:
        print("Collection not provided. Use --collection or set 'collection' in pipeline_config.json")
        return 1

    env = os.environ.copy()
    config_force = bool(cfg.get("force_refresh", False))
    env["FORCE_REFRESH"] = "1" if (args.force or config_force) else "0"
    env["COLLECTION_SLUG"] = normalize_collection_slug(str(collection_value))

    config_api_key = str(cfg.get("opensea_api_key", "")).strip()
    if config_api_key and not env.get("OPENSEA_API_KEY"):
        env["OPENSEA_API_KEY"] = config_api_key

    scripts = build_scripts()

    print(f"Collection : {env['COLLECTION_SLUG']}")
    print(f"Level 3    : {LEVEL3_SCRIPT}")

    if not env.get("OPENSEA_API_KEY"):
        print("Warning: OPENSEA_API_KEY not set. Falling back to key inside scripts.")

    for script in build_scripts():
        code = run_script(script, env)
        if code != 0:
            print(f"\nPipeline stopped at {script} (exit code {code}).")
            return code

    print("\nPipeline complete: Level1 -> Level4")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
