import os
import time
import logging
import gzip
import base64
import json
from pathlib import Path
from typing import Dict, List, Optional

import toml
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.types import Integer, Float, String, Date, DateTime, Numeric
from sqlalchemy.engine import Connection

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
USE_COMPRESSION = True  # GZIP payload for density


def configure_logging(level_str: str) -> None:
    if not level_str:
        return
    level_str = level_str.upper()
    level = getattr(logging, level_str, None)
    if not isinstance(level, int):
        log.warning(f"Invalid log level '{level_str}'. Defaulting to INFO.")
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )
    log.info(f"Logging level set to {logging.getLevelName(level)}")


def normalize_header(raw: str) -> str:
    """Normalize column headers to SQL-friendly names"""
    if not raw or not raw.strip():
        raw = "Column"
    s = raw.strip()
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = "".join(ch if ch.isalnum() else "_" for ch in s)
    if not s or s.isspace():
        s = "Column"
    return s


def infer_max_text_lengths(df: pd.DataFrame) -> Dict[str, int]:
    """Infer maximum text length for string columns"""
    max_lens = {}
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(
            df[col]
        ):
            max_len = df[col].dropna().astype(str).map(len).max()
            max_lens[col] = int(max_len) if pd.notna(max_len) and max_len > 0 else 1
    return max_lens


def coerce_pandas_dtypes(df: pd.DataFrame, dtypes_cfg: Dict) -> pd.DataFrame:
    """Coerce DataFrame columns to specified dtypes"""
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
    """Build SQLAlchemy dtype map for table creation"""
    dtype_map = {}
    for col in max_text_lens:
        if col in dtypes_cfg:
            dt = dtypes_cfg[col].upper()
            sa_type_cls = SQLALCHEMY_TYPE_MAP.get(dt, String)
            if dt in ("TEXT", "STRING"):
                size = max_text_lens[col]
                if size <= MAX_FIXED_LENGTH:
                    dtype_map[col] = String(size)
                else:
                    dtype_map[col] = String()  # NVARCHAR(MAX)
            else:
                dtype_map[col] = sa_type_cls()
        else:
            size = max_text_lens[col]
            if size <= MAX_FIXED_LENGTH:
                dtype_map[col] = String(size)
            else:
                dtype_map[col] = String()  # NVARCHAR(MAX)

    for col, dtype in (dtypes_cfg or {}).items():
        if col not in dtype_map:
            dt = dtype.upper()
            sa_type_cls = SQLALCHEMY_TYPE_MAP.get(dt, String)
            dtype_map[col] = sa_type_cls()
    return dtype_map


def create_table_if_not_exists(
    conn: Connection, table_name: str, df: pd.DataFrame, dtype_map: Dict
):
    """Create table if it doesn't exist"""
    metadata = MetaData()
    columns = []
    for col in df.columns:
        col_type = dtype_map.get(col, String())
        columns.append(Column(col, col_type, nullable=True))
    table = Table(table_name, metadata, *columns)
    metadata.create_all(bind=conn)
    log.info(f"Ensured table '{table_name}' exists.")


def df_to_json_array(df: pd.DataFrame) -> str:
    """Convert DataFrame to JSON array string"""
    return df.to_json(orient="records", date_format="iso", force_ascii=False)


def build_openjson_with_clause(
    dtypes_cfg: Dict[str, str], df_columns: List[str], max_text_lens: Dict[str, int]
) -> str:
    """Build the WITH clause for OPENJSON"""
    lines = []
    for col in df_columns:
        if dtypes_cfg and col in dtypes_cfg:
            dtype = dtypes_cfg[col].upper()
            sql_type = OPENJSON_SQL_TYPE_MAP.get(dtype)
            if not sql_type:
                # Default to NVARCHAR with appropriate size
                size = max_text_lens.get(col, 1)
                if size <= MAX_FIXED_LENGTH:
                    sql_type = f"NVARCHAR({size})"
                else:
                    sql_type = "NVARCHAR(MAX)"
        else:
            # Use inferred max length for strings
            size = max_text_lens.get(col, 1)
            if size <= MAX_FIXED_LENGTH:
                sql_type = f"NVARCHAR({size})"
            else:
                sql_type = "NVARCHAR(MAX)"

        # Escape column name and JSON path
        escaped_col = col.replace("]", "]]")
        lines.append(f"[{escaped_col}] {sql_type} '$.{col}'")

    if not lines:
        raise ValueError(
            "Empty WITH clause generated for OPENJSON; check dtype configuration."
        )
    return ",\n        ".join(lines)


def compress_gzip(data: bytes) -> bytes:
    """Compress data using GZIP"""
    return gzip.compress(data, compresslevel=6)


def insert_json_batch_with_compression(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    dtypes_cfg: Dict[str, str],
    max_text_lens: Dict[str, int],
    use_compression: bool = USE_COMPRESSION,
):
    """Insert batch using OPENJSON with optional GZIP compression (like Class1.cs)"""
    json_data = df_to_json_array(df)

    # Build the SQL statement similar to Class1.cs
    if use_compression:
        # Encode to UTF-16 (NVARCHAR uses UTF-16) and compress
        json_bytes = json_data.encode("utf-16-le")
        gzipped = compress_gzip(json_bytes)
        base64_payload = base64.b64encode(gzipped).decode("ascii")

        # Build SQL with GZIP decompression (like Class1.cs lines 360-362)
        sql = f"""
DECLARE @payload_base64 nvarchar(max) = N'{base64_payload.replace("'", "''")}';
DECLARE @bin varbinary(max) = CAST(N'' as xml).value('xs:base64Binary(sql:variable("@payload_base64"))', 'varbinary(max)');
DECLARE @json nvarchar(max) = CAST(DECOMPRESS(@bin) AS nvarchar(max));
"""
    else:
        # No compression - direct JSON
        escaped_json = json_data.replace("'", "''")
        sql = f"""
DECLARE @json nvarchar(max) = N'{escaped_json}';
"""

    # Build WITH clause and column list
    with_clause = build_openjson_with_clause(dtypes_cfg, list(df.columns), max_text_lens)
    columns_list = ", ".join(f"[{col.replace(']', ']]')}]" for col in df.columns)

    # Complete the INSERT statement (like Class1.cs lines 379-384)
    sql += f"""
INSERT INTO {table_name} WITH (TABLOCK) ({columns_list})
SELECT {columns_list}
FROM OPENJSON(@json)
WITH (
        {with_clause}
) AS j;
"""

    conn.execute(text(sql))
    conn.commit()


def adaptive_batch_insert_with_ewma(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    dtypes_cfg: Dict[str, str],
    max_text_lens: Dict[str, int],
    starting_batch: str = "10000",
    min_batch: int = 5000,
    max_batch: int = 150000,
    target_seconds: float = 3.0,
):
    """
    Adaptive batch insert with EWMA tracking (based on Class1.cs lines 66-150)

    Args:
        starting_batch: Starting batch size. Prefix with '=' for fixed batch (e.g., '=5000')
    """
    total_rows = len(df)
    sent = 0

    # Parse starting batch (support '=' prefix for fixed batch like C# version)
    fixed_batch = starting_batch.startswith("=")
    batch = int(starting_batch.lstrip("="))

    # Adaptive controls (from Class1.cs lines 76-81)
    fast_threshold = 0.75 * target_seconds  # 2.25s
    slow_threshold = 1.50 * target_seconds  # 4.5s
    inc_factor = 1.20
    dec_factor = 0.80
    ewma_alpha = 0.25  # smoothing for rows/sec
    ewma_rows_per_sec = 0.0

    log.info(
        f"Starting adaptive batch insert: total={total_rows}, starting_batch={batch}, "
        f"fixed={fixed_batch}, min={min_batch}, max={max_batch}, target={target_seconds}s"
    )

    while sent < total_rows:
        remaining = total_rows - sent
        take = min(batch, remaining)
        batch_df = df.iloc[sent : sent + take]

        # Execute with timing (like Class1.cs lines 92-95)
        start_time = time.perf_counter()
        try:
            insert_json_batch_with_compression(
                conn, table_name, batch_df, dtypes_cfg, max_text_lens, USE_COMPRESSION
            )
        except Exception as e:
            # Error-aware backoff (like Class1.cs lines 102-120)
            elapsed = time.perf_counter() - start_time
            err_msg = str(e).lower()
            log.error(
                f"Batch failed (rows={take}, {elapsed:.2f}s). Error: {e}"
            )

            batch = max(int(batch * dec_factor), min_batch)
            log.info(f"Reducing batch size to {batch} due to error")

            # More aggressive reduction for specific errors (Class1.cs lines 112-117)
            if any(
                keyword in err_msg
                for keyword in ["timeout", "request size", "payload too large"]
            ):
                batch = max(int(batch * dec_factor), min_batch)
                log.info(f"Further reducing batch size to {batch} (error-specific)")

            # Do not advance 'sent'; retry this segment
            continue

        end_time = time.perf_counter()
        elapsed = max(end_time - start_time, 0.001)

        # Calculate rows/sec and update EWMA (Class1.cs lines 97-100)
        rows_per_sec = take / elapsed
        if ewma_rows_per_sec == 0.0:
            ewma_rows_per_sec = rows_per_sec
        else:
            ewma_rows_per_sec = (
                ewma_alpha * rows_per_sec + (1.0 - ewma_alpha) * ewma_rows_per_sec
            )

        # Success: commit progress (Class1.cs line 124)
        sent += take
        log.info(
            f"Inserted {sent}/{total_rows}. Batch={take}, {elapsed:.2f}s "
            f"({rows_per_sec:.0f} rows/s, EWMA={ewma_rows_per_sec:.0f} rows/s)"
        )

        # Adaptive tuning on success (Class1.cs lines 129-149)
        if not fixed_batch:
            if elapsed < fast_threshold:
                # Fast → increase (Class1.cs lines 130-135)
                new_batch = min(int(batch * inc_factor), max_batch)
                if new_batch != batch:
                    log.info(
                        f"Increasing batch size from {batch} to {new_batch} (fast: {elapsed:.2f}s)"
                    )
                    batch = new_batch
            elif elapsed > slow_threshold:
                # Slow → decrease (Class1.cs lines 136-141)
                new_batch = max(int(batch * dec_factor), min_batch)
                if new_batch != batch:
                    log.info(
                        f"Decreasing batch size from {batch} to {new_batch} (slow: {elapsed:.2f}s)"
                    )
                    batch = new_batch
            else:
                # Within target band: small additive increase (Class1.cs lines 144-149)
                additive = max(batch // 20, 1000)  # +5% or 1000 min
                new_batch = min(batch + additive, max_batch)
                if new_batch != batch:
                    batch = new_batch


def load_csv_with_polars_lazy(
    src_path: str,
    delimiter: str = ",",
    null_values: Optional[List[str]] = None,
    column_mappings: Optional[Dict[str, str]] = None,
    table_name: str = "table",
) -> pd.DataFrame:
    """Load CSV using Polars for efficiency, return as Pandas DataFrame"""
    log.info(f"[{table_name}] Loading CSV: {src_path}")

    # Use Polars lazy API for efficient loading
    df = pl.scan_csv(
        src_path,
        separator=delimiter,
        null_values=null_values,
        ignore_errors=True,
        try_parse_dates=True,
    )

    # Apply column mappings if provided
    if column_mappings:
        # Rename columns according to mappings
        rename_dict = {k: v for k, v in column_mappings.items() if k in df.columns}
        if rename_dict:
            df = df.rename(rename_dict)

    # Collect to Pandas
    pandas_df = df.collect().to_pandas()

    # Normalize column names
    pandas_df.columns = [normalize_header(col) for col in pandas_df.columns]

    log.info(
        f"[{table_name}] Loaded {len(pandas_df)} rows, {len(pandas_df.columns)} columns from CSV"
    )
    return pandas_df


def load_excel_with_pandas(
    src_path: str,
    sheet: Optional[str] = None,
    column_mappings: Optional[Dict[str, str]] = None,
    table_name: str = "table",
) -> pd.DataFrame:
    """Load Excel file using Pandas"""
    log.info(f"[{table_name}] Loading Excel: {src_path}")

    # Load Excel file
    if sheet:
        df = pd.read_excel(src_path, sheet_name=sheet)
    else:
        df = pd.read_excel(src_path)

    # Apply column mappings if provided
    if column_mappings:
        rename_dict = {k: v for k, v in column_mappings.items() if k in df.columns}
        if rename_dict:
            df = df.rename(columns=rename_dict)

    # Normalize column names
    df.columns = [normalize_header(col) for col in df.columns]

    log.info(
        f"[{table_name}] Loaded {len(df)} rows, {len(df.columns)} columns from Excel"
    )
    return df


def run_pipeline(
    config_path: str,
    only_tables: Optional[List[str]] = None,
    starting_batch_size: str = "10000",
):
    """Main pipeline to load data from config into SQL Server"""
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    log.info(f"Reading config: {config_path}")
    config = toml.load(config_path)

    db_url = config["database"]["url"]
    log.info(f"Creating engine for DB URL: {db_url}")

    engine = create_engine(db_url, pool_pre_ping=True, fast_executemany=True)

    options = config.get("options", {}) or {}
    toml_log_level = options.get("loglevel")
    cli_log_level = os.getenv("LOG_LEVEL")

    if cli_log_level:
        configure_logging(cli_log_level)
    elif toml_log_level:
        configure_logging(toml_log_level)
    else:
        configure_logging("INFO")

    # Use starting_batch_size from CLI or config
    default_batch = starting_batch_size or str(options.get("chunksize", 10000))
    log.info(f"Global options: starting_batch={default_batch}")

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

    # First pass: create all tables
    log.info("=" * 60)
    log.info("PHASE 1: Creating tables")
    log.info("=" * 60)

    with engine.connect() as conn:
        for tbl in tables:
            name = tbl["name"]
            src_path = tbl["source_path"]
            src_type = tbl["source_type"].lower()
            mappings = tbl.get("column_mappings", {}) or {}
            dtypes_cfg = tbl.get("dtypes", {}) or {}

            log.info(f"\n[{name}] Loading data for schema inference...")

            # Load data
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

            # Coerce dtypes and infer max lengths
            df = coerce_pandas_dtypes(df, dtypes_cfg)
            max_text_lens = infer_max_text_lengths(df)
            dtype_map = build_sqlalchemy_dtype_map(dtypes_cfg, max_text_lens)

            # Create table
            create_table_if_not_exists(conn, name, df, dtype_map)

        conn.commit()

    # Second pass: insert data with adaptive batching
    log.info("\n" + "=" * 60)
    log.info("PHASE 2: Inserting data with adaptive batching")
    log.info("=" * 60)

    with engine.connect() as conn:
        for tbl in tables:
            name = tbl["name"]
            src_path = tbl["source_path"]
            src_type = tbl["source_type"].lower()
            mappings = tbl.get("column_mappings", {}) or {}
            dtypes_cfg = tbl.get("dtypes", {}) or {}

            log.info(f"\n[{name}] Loading data for insert...")

            # Load data again (to avoid keeping in memory)
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

            if len(df) == 0:
                log.warning(f"[{name}] DataFrame has 0 rows after load. Skipping.")
                continue

            # Coerce dtypes and infer max lengths
            df = coerce_pandas_dtypes(df, dtypes_cfg)
            max_text_lens = infer_max_text_lengths(df)

            # Insert with adaptive batching
            log.info(f"[{name}] Starting adaptive batch insert of {len(df)} rows...")
            adaptive_batch_insert_with_ewma(
                conn, name, df, dtypes_cfg, max_text_lens, starting_batch=default_batch
            )

            log.info(f"[{name}] Completed insert.")

    log.info("\n" + "=" * 60)
    log.info("Pipeline complete!")
    log.info("=" * 60)
