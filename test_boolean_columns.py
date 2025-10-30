#!/usr/bin/env python3
"""Test to verify boolean columns are created as NVARCHAR, not INT"""

import pandas as pd
from src.data_loader.core import infer_column_info, build_openjson_with_clause

print("=" * 80)
print("BOOLEAN COLUMN TYPE TEST")
print("=" * 80)

# Create DataFrame with boolean columns
df = pd.DataFrame({
    'id': [1, 2, 3],
    'is_active': [True, False, True],
    'is_deleted': [False, False, True],
    'has_access': [True, True, False],
})

print("\nSample DataFrame with boolean columns:")
print(df)
print("\nDataFrame dtypes:")
print(df.dtypes)

print("\n" + "=" * 80)

# Infer column info
col_info = infer_column_info(df)

print("\nInferred Column Info:")
print("=" * 80)
for col, info in col_info.items():
    print(f"{col:15} | Type: {info['type']:10} | Size: {info['size']}")

print("\n" + "=" * 80)

# Generate OPENJSON WITH clause
dtypes_cfg = {}
with_clause = build_openjson_with_clause(dtypes_cfg, list(df.columns), col_info)

print("\nGenerated OPENJSON WITH clause:")
print("=" * 80)
print(with_clause)

print("\n" + "=" * 80)
print("\nVERIFICATION:")
print("=" * 80)

# Check that all boolean columns are NVARCHAR, not INT
success = True
for col in ['is_active', 'is_deleted', 'has_access']:
    col_type = col_info[col]['type']
    col_size = col_info[col]['size']

    if col_type == 'STRING' and col_size == 10:
        print(f"✅ {col:15} → NVARCHAR(10) (correct)")
    else:
        print(f"❌ {col:15} → {col_type} (incorrect, should be STRING/NVARCHAR)")
        success = False

print("\n" + "=" * 80)

if success:
    print("\n✅ ALL BOOLEAN COLUMNS CORRECTLY MAPPED TO NVARCHAR(10)")
    print("Boolean values (True/False) will be stored as text in database")
else:
    print("\n❌ SOME BOOLEAN COLUMNS INCORRECTLY MAPPED TO INT")

print("=" * 80)

# Show what the actual data looks like when converted to JSON
print("\nBoolean representation in JSON:")
print("=" * 80)
print(df.to_json(orient="records"))
print("\nNote: JSON booleans (true/false) will be stored as text in NVARCHAR columns")
