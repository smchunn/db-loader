import sys
import os
import argparse
from data_loader.core import run_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="Efficient CSV/Excel -> MS SQL Server loader with adaptive batching"
    )
    parser.add_argument(
        "--config", type=str, default="config.toml", help="Path to TOML config file"
    )
    parser.add_argument(
        "--tables", type=str, nargs="*", help="Optional list of table names to process"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        help="Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "--starting-batch",
        type=str,
        default="10000",
        help="Starting batch size (prefix with '=' for fixed batch, e.g., '=5000')",
    )
    args = parser.parse_args()

    # CLI precedence: if provided, set env for core
    if args.log_level:
        os.environ["LOG_LEVEL"] = args.log_level.upper()

    run_pipeline(
        args.config, only_tables=args.tables, starting_batch_size=args.starting_batch
    )


if __name__ == "__main__":
    main()
