#!/usr/bin/env python3
"""Test script to demonstrate column type inference"""

import pandas as pd
from src.data_loader.core import infer_column_info, build_openjson_with_clause, OPENJSON_SQL_TYPE_MAP

# Create a sample DataFrame with mixed types
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000.50, 60000.75, 70000.25],
    'hire_date': pd.to_datetime(['2020-01-01', '2021-02-15', '2022-03-20']),
    'is_active': [True, False, True],
    'description': ['Engineer' * 100, 'Manager' * 200, 'Director'],  # Long strings
})

print("Sample DataFrame:")
print(df.dtypes)
print("\n" + "=" * 80)

# Test column inference
col_info = infer_column_info(df)

print("\nInferred Column Info:")
print("=" * 80)
for col, info in col_info.items():
    print(f"{col:20} | Type: {info['type']:10} | Size: {info['size']}")

print("\n" + "=" * 80)

# Test OPENJSON WITH clause generation
dtypes_cfg = {}  # No explicit config
with_clause = build_openjson_with_clause(dtypes_cfg, list(df.columns), col_info)

print("\nGenerated OPENJSON WITH clause:")
print("=" * 80)
print(with_clause)

print("\n" + "=" * 80)
print("\nExpected SQL types from OPENJSON_SQL_TYPE_MAP:")
for dtype, sql_type in OPENJSON_SQL_TYPE_MAP.items():
    print(f"  {dtype:15} -> {sql_type}")
