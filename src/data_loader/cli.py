import sys
import os
import argparse
from data_loader.core import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="CSV/Excel -> DB loader")
    parser.add_argument(
        "--config", type=str, default="config.toml", help="Path to TOML config"
    )
    parser.add_argument(
        "--tables", type=str, nargs="*", help="Optional list of table names to process"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        help="Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    args = parser.parse_args()

    # CLI precedence: if provided, set env for core
    if args.log_level:
        os.environ["LOG_LEVEL"] = args.log_level.upper()

    run_pipeline(args.config, only_tables=args.tables)


if __name__ == "__main__":
    main()
