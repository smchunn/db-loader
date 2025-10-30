import os
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional

import toml
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, MetaData, Table, Column, text, event
from sqlalchemy.types import Integer, Float, String, Date, DateTime, Numeric
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError
from tqdm import tqdm

log = logging.getLogger("data_loader.core")

# SQL type mapping for table creation
SQLALCHEMY_TYPE_MAP = {
    "INT": Integer,
    "INTEGER": Integer,
    "FLOAT": Float,
    "DOUBLE": Float,
    "NUMERIC": Numeric,
    "DECIMAL": Numeric,
    "TEXT": String,
    "STRING": String,
    "DATE": Date,
    "DATETIME": DateTime,
    "TIMESTAMP": DateTime,
}

# SQL Server types for OPENJSON WITH clause
OPENJSON_SQL_TYPE_MAP = {
    "INT": "INT",
    "INTEGER": "INT",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "NUMERIC": "NUMERIC(18,4)",
    "DECIMAL": "NUMERIC(18,4)",
    "TEXT": "NVARCHAR(MAX)",
    "STRING": "NVARCHAR(MAX)",
    "DATE": "DATE",
    "DATETIME": "DATETIME2",
    "TIMESTAMP": "DATETIME2",
}

MAX_FIXED_LENGTH = 4000  # Max NVARCHAR size before NVARCHAR(MAX)


def configure_logging(level_str: str) -> None:
    if not level_str:
        return
    level_str = level_str.upper()
    level = getattr(logging, level_str, None)
    if not isinstance(level, int):
        log.warning(f"Invalid log level '{level_str}'. Defaulting to INFO.")
        level = logging.INFO
    logging.getLogger().setLevel(level)
    for logger_name in ("data_loader.core",):
        logging.getLogger(logger_name).setLevel(level)
    log.info(f"Logging level set to {logging.getLevelName(level)}")


def infer_max_text_lengths(df: pd.DataFrame) -> Dict[str, int]:
    max_lens = {}
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            max_len = df[col].dropna().map(len).max()
            max_lens[col] = max_len if max_len is not None else 1
    return max_lens


def coerce_pandas_dtypes(df: pd.DataFrame, dtypes_cfg: Dict) -> pd.DataFrame:
    if not dtypes_cfg:
        return df
    for col, dtype in dtypes_cfg.items():
        if col not in df.columns:
            log.warning(f"Column '{col}' not found for dtype coercion.")
            continue
        dt = dtype.upper()
        try:
            if dt in ("INT", "INTEGER"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dt in ("FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dt == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dt in ("DATETIME", "TIMESTAMP"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dt in ("TEXT", "STRING"):
                df[col] = df[col].astype("string")
        except Exception as e:
            log.warning(f"Failed to coerce column '{col}' to {dtype}: {e}")
    return df


def build_sqlalchemy_dtype_map(dtypes_cfg: Dict, max_text_lens: Dict[str, int]) -> Dict:
    dtype_map = {}
    for col in max_text_lens:
        if col in dtypes_cfg:
            dt = dtypes_cfg[col].upper()
            sa_type_cls = SQLALCHEMY_TYPE_MAP.get(dt, String)
            if dt in ("TEXT", "STRING"):
                # Use NVARCHAR with size from max_text_lens if <= MAX_FIXED_LENGTH
                size = max_text_lens[col]
                if size <= MAX_FIXED_LENGTH:
                    dtype_map[col] = String(size)
                else:
                    dtype_map[col] = String()  # NVARCHAR(MAX)
            else:
                dtype_map[col] = sa_type_cls()
        else:
            # Default string columns to NVARCHAR with max length
            size = max_text_lens[col]
            if size <= MAX_FIXED_LENGTH:
                dtype_map[col] = String(size)
            else:
                dtype_map[col] = String()  # NVARCHAR(MAX)
    # For columns not string type, map from config if present
    for col, dtype in (dtypes_cfg or {}).items():
        if col not in dtype_map:
            dt = dtype.upper()
            sa_type_cls = SQLALCHEMY_TYPE_MAP.get(dt, String)
            dtype_map[col] = sa_type_cls()
    return dtype_map


def create_table_if_not_exists(
    conn: Connection, table_name: str, df: pd.DataFrame, dtype_map: Dict
):
    metadata = MetaData()
    columns = []
    for col in df.columns:
        col_type = dtype_map.get(col, String())
        columns.append(Column(col, col_type))
    table = Table(table_name, metadata, *columns)
    metadata.create_all(bind=conn)
    log.info(f"Ensured table '{table_name}' exists.")


def df_to_json_array(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso")


def build_openjson_with_clause(
    dtypes_cfg: Dict[str, str], df_columns: List[str]
) -> str:
    lines = []
    if dtypes_cfg:
        for col in df_columns:
            dtype = dtypes_cfg.get(col, "NVARCHAR(MAX)").upper()
            sql_type = OPENJSON_SQL_TYPE_MAP.get(dtype, "NVARCHAR(MAX)")
            lines.append(f"[{col}] {sql_type} '$.{col}'")
    else:
        for col in df_columns:
            lines.append(f"[{col}] NVARCHAR(MAX) '$.{col}'")
    if not lines:
        raise ValueError(
            "Empty WITH clause generated for OPENJSON; check dtype configuration."
        )
    return ",\n    ".join(lines)


def insert_json_batch(
    conn: Connection, table_name: str, df: pd.DataFrame, dtypes_cfg: Dict[str, str]
):
    json_data = df_to_json_array(df)
    with_clause = build_openjson_with_clause(dtypes_cfg, list(df.columns))
    columns_list = ", ".join(f"[{col}]" for col in df.columns)
    sql = f"""
    INSERT INTO {table_name} ({columns_list})
    SELECT {columns_list}
    FROM OPENJSON(:json_data)
    WITH (
        {with_clause}
    )
    """
    conn.execute(text(sql), {"json_data": json_data})


def adaptive_batch_insert(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    dtypes_cfg: Dict[str, str],
    starting_batch: int = 5000,
    min_batch: int = 1000,
    max_batch: int = 50000,
    target_seconds: float = 3.0,
):
    total_rows = len(df)
    sent = 0
    batch = starting_batch
    ewma_alpha = 0.25
    ewma_rows_per_sec = 0.0
    fast_threshold = 0.75 * target_seconds
    slow_threshold = 1.5 * target_seconds
    inc_factor = 1.2
    dec_factor = 0.8

    while sent < total_rows:
        remaining = total_rows - sent
        take = min(batch, remaining)
        batch_df = df.iloc[sent : sent + take]

        start_time = time.perf_counter()
        try:
            insert_json_batch(conn, table_name, batch_df, dtypes_cfg)
        except Exception as e:
            log.error(f"Batch insert failed at rows {sent}-{sent+take}: {e}")
            batch = max(int(batch * dec_factor), min_batch)
            log.info(f"Reducing batch size to {batch} due to error.")
            continue
        end_time = time.perf_counter()

        elapsed = max(end_time - start_time, 0.001)
        rows_per_sec = take / elapsed
        ewma_rows_per_sec = (
            rows_per_sec
            if ewma_rows_per_sec == 0
            else (ewma_alpha * rows_per_sec + (1 - ewma_alpha) * ewma_rows_per_sec)
        )

        sent += take
        log.info(
            f"Inserted {sent}/{total_rows} rows in {elapsed:.2f}s ({rows_per_sec:.0f} r/s, EWMA {ewma_rows_per_sec:.0f} r/s), batch size {batch}"
        )

        if elapsed < fast_threshold:
            batch = min(int(batch * inc_factor), max_batch)
            log.info(f"Increasing batch size to {batch} (fast batch)")
        elif elapsed > slow_threshold:
            batch = max(int(batch * dec_factor), min_batch)
            log.info(f"Decreasing batch size to {batch} (slow batch)")


def run_pipeline(config_path: str, only_tables: Optional[List[str]] = None):
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    log.info(f"Reading config: {config_path}")
    config = toml.load(config_path)

    db_url = config["database"]["url"]
    log.info(f"Creating engine for DB URL: {db_url}")

    engine = create_engine(db_url, pool_pre_ping=True)

    options = config.get("options", {}) or {}
    toml_log_level = options.get("loglevel")
    cli_log_level = os.getenv("LOG_LEVEL")
    if not cli_log_level and toml_log_level:
        configure_logging(toml_log_level)
    else:
        if cli_log_level:
            log.debug(f"CLI LOG_LEVEL in effect: {cli_log_level}")
        elif not toml_log_level:
            log.debug("No explicit log level provided; using default INFO.")

    chunksize = int(options.get("chunksize", 5000))
    log.info(f"Global options: chunksize={chunksize}")

    tables = config.get("tables", [])
    if not tables:
        log.warning("No tables configured in TOML; nothing to process.")
        return

    if only_tables:
        names = [t.get("name") for t in tables]
        log.info(f"Filtering tables: only_tables={only_tables} from available={names}")
        tables = [t for t in tables if t.get("name") in only_tables]
        if not tables:
            log.warning(f"No matching tables for filters: {only_tables}")
            return

    with engine.connect() as conn:
        for tbl in tables:
            name = tbl["name"]
            src_path = tbl["source_path"]
            src_type = tbl["source_type"].lower()
            mappings = tbl.get("column_mappings", {}) or {}
            dtypes_cfg = tbl.get("dtypes", {}) or {}

            if src_type == "csv":
                df = load_csv_with_polars_lazy(
                    src_path,
                    delimiter=tbl.get("delimiter", ","),
                    null_values=tbl.get("null_values", None),
                    column_mappings=mappings,
                    table_name=name,
                )
            elif src_type == "excel":
                df = load_excel_with_pandas(
                    src_path,
                    sheet=tbl.get("sheet", None),
                    column_mappings=mappings,
                    table_name=name,
                )
            else:
                raise ValueError(f"Unsupported source_type: {src_type}")

            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()

            df = coerce_pandas_dtypes(df, dtypes_cfg)
            max_text_lens = infer_max_text_lengths(df)
            dtype_map = build_sqlalchemy_dtype_map(dtypes_cfg, max_text_lens)

            create_table_if_not_exists(conn, name, df, dtype_map)

    with engine.begin() as conn:
        for tbl in tables:
            name = tbl["name"]
            src_path = tbl["source_path"]
            src_type = tbl["source_type"].lower()
            mappings = tbl.get("column_mappings", {}) or {}
            dtypes_cfg = tbl.get("dtypes", {}) or {}

            if src_type == "csv":
                df = load_csv_with_polars_lazy(
                    src_path,
                    delimiter=tbl.get("delimiter", ","),
                    null_values=tbl.get("null_values", None),
                    column_mappings=mappings,
                    table_name=name,
                )
            elif src_type == "excel":
                df = load_excel_with_pandas(
                    src_path,
                    sheet=tbl.get("sheet", None),
                    column_mappings=mappings,
                    table_name=name,
                )
            else:
                raise ValueError(f"Unsupported source_type: {src_type}")

            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
            elif not isinstance(df, pd.DataFrame):
                raise TypeError(f"Expected Pandas DataFrame, got {type(df)}")

            if len(df) == 0:
                log.warning(f"[{name}] DataFrame has 0 rows after load.")

            df = coerce_pandas_dtypes(df, dtypes_cfg)

            log.info(f"[{name}] Starting adaptive batch insert of {len(df)} rows...")
            adaptive_json_batch_insert(
                conn, name, df, dtypes_cfg, starting_batch=chunksize
            )

            log.info(f"[{name}] Completed insert.")


# Implement or import your load_csv_with_polars_lazy and load_excel_with_pandas functions as before.

# You can run the pipeline with:
# run_pipeline("your_config.toml")
