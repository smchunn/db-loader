# db-loader

High-performance CSV/Excel to MS SQL Server data loader with adaptive batching, GZIP compression, and EWMA-based throughput optimization.

## Features

- **Adaptive Batch Sizing** - Automatically adjusts batch size based on insert performance (EWMA tracking)
- **GZIP Compression** - Compresses JSON payloads before sending to reduce network overhead
- **OPENJSON Inserts** - Uses SQL Server's native JSON parsing for fast bulk inserts
- **Smart Type Inference** - Automatically detects column types from DataFrame (int, float, datetime, string, boolean)
- **Error Resilience** - Retry logic with fallback to row-by-row insert on persistent failures
- **Config-Driven** - Define all tables and mappings in a single TOML file

## Requirements

- Python >= 3.9
- MS SQL Server with OPENJSON support (2016+)

## Installation

```bash
# Clone or navigate to the project directory
cd db-loader

# Install in development mode
pip install -e .
```

## Usage

### 1. Create a config file

Create `config.toml` with your database connection and table definitions:

```toml
[database]
url = "mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"

[options]
chunksize = 10000      # Starting batch size (adaptive)
loglevel = "INFO"      # DEBUG, INFO, WARNING, ERROR, CRITICAL

[[tables]]
name = "customers"
source_path = "/path/to/customers.csv"
source_type = "csv"
delimiter = ","

# Optional: rename columns
column_mappings = {
    "Customer ID" = "customer_id",
    "First Name" = "first_name"
}

# Optional: explicit type definitions
dtypes = {
    "customer_id" = "INTEGER",
    "revenue" = "FLOAT",
    "signup_date" = "DATETIME"
}

[[tables]]
name = "orders"
source_path = "/path/to/orders.xlsx"
source_type = "excel"
sheet = "Sheet1"
```

### 2. Run the loader

```bash
# Load all tables from config
python -m data_loader --config config.toml

# Or use the installed command
loader --config config.toml

# Load specific tables only
loader --config config.toml --tables customers orders

# Use fixed batch size (prefix with '=')
loader --config config.toml --starting-batch =5000

# Enable debug logging
loader --config config.toml --log-level DEBUG
```

## Configuration Options

### Database URL Format

```
mssql+pyodbc://[user[:password]@]host/database[?driver=...&option=...]
```

### Table Options

| Option | Type | Description |
|--------|------|-------------|
| `name` | string | Table name in database |
| `source_path` | string | Path to CSV or Excel file |
| `source_type` | string | "csv" or "excel" |
| `delimiter` | string | CSV delimiter (default: ",") |
| `sheet` | string | Excel sheet name (for Excel files) |
| `column_mappings` | dict | Rename columns: `{"old_name" = "new_name"}` |
| `dtypes` | dict | Explicit types: `{"col" = "INTEGER/FLOAT/DATETIME/STRING"}` |
| `if_exists` | string | How to handle existing table: "append" (default), "replace", or "skip" |

### CLI Options

| Option | Description |
|--------|-------------|
| `--config PATH` | Path to TOML config file (default: config.toml) |
| `--tables NAME [NAME ...]` | Process only specified tables |
| `--starting-batch SIZE` | Starting batch size (prefix with = for fixed) |
| `--log-level LEVEL` | Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |

### Table Handling with `if_exists`

The `if_exists` option controls what happens when a table already exists:

- **`append`** (default) - Add data to the existing table without modifying it
- **`replace`** - Drop the existing table and recreate it with new schema
- **`skip`** - Skip this table entirely if it already exists (useful for incremental loads)

Example:
```toml
[[tables]]
name = "customers"
source_path = "customers.csv"
source_type = "csv"
if_exists = "skip"  # Don't reload if table already exists
```

## How It Works

1. **Table Creation** - Infers column types from data, creates tables with appropriate NVARCHAR/INT/FLOAT/DATETIME2 types
2. **Adaptive Batching** - Starts with configured batch size, adjusts based on insert speed
3. **GZIP Compression** - Encodes JSON as UTF-16, compresses with GZIP, base64 encodes for SQL
4. **OPENJSON Insert** - Uses SQL Server's `OPENJSON()` function for fast bulk inserts with `TABLOCK` hint
5. **EWMA Tracking** - Monitors throughput (rows/sec) with exponentially weighted moving average
6. **Error Handling** - Reduces batch size on errors, falls back to row-by-row insert after max retries

## Performance

Typical throughput: **50,000-150,000 rows/second** depending on:
- Network latency
- Column count and complexity
- Server load
- Data compression ratio

The adaptive batching automatically finds the optimal batch size for your environment.

## Example Output

```
INFO: Starting adaptive batch insert: total=1000000, starting_batch=10000, target=3.0s
INFO: Inserted 10000/1000000. Batch=10000, 2.15s (4651 rows/s, EWMA=4651 rows/s)
INFO: Increasing batch size from 10000 to 12000 (fast: 2.15s)
INFO: Inserted 22000/1000000. Batch=12000, 2.78s (4317 rows/s, EWMA=4567 rows/s)
INFO: Inserted 1000000/1000000. Batch=45000, 2.94s (15306 rows/s, EWMA=12450 rows/s)
INFO: Pipeline complete!
```

## Troubleshooting

**Error: "Cannot convert JSON boolean to INT"**
- Boolean columns are automatically stored as NVARCHAR(10) with "true"/"false" text

**Hanging on insert**
- Check max_retries limit (default: 5), increases batch size or reduces on errors
- Enable DEBUG logging to see detailed error messages

**Slow performance**
- Increase starting batch size: `--starting-batch 50000`
- Check network latency to SQL Server
- Verify SQL Server has sufficient resources

## License

MIT
