# Parquet to BigQuery Schema Generator

## Overview

This tool reads Parquet files and generates:
- BigQuery CREATE TABLE DDL statements
- Column name and data type tuples



## Usage

### As a Script

Run directly from the command line:

```bash
python3 scripts/parquet_to_bq_schema.py
```

*Note: The script currently has a hardcoded path. Modify the `__main__` section to use your Parquet file.*

### As a Module

Import and use in your Python code:

```python
from scripts.parquet_to_bq_schema import generate_bigquery_schema_from_parquet

# Generate schema from Parquet file
ddl_statement, column_tuples = generate_bigquery_schema_from_parquet(
    parquet_file="path/to/your/file.parquet",
    table_name="your_table_name"
)

print("CREATE TABLE DDL:")
print(ddl_statement)

print("\nColumn Information:")
for name, datatype in column_tuples:
    print(f"  {name}: {datatype}")
```

### Using the Class Directly

For more control over the process:

```python
from scripts.parquet_to_bq_schema import BigQuerySchemaGenerator

generator = BigQuerySchemaGenerator()

# Generate schema
schema = generator.generate_schema("path/to/file.parquet")

# Print summary of what was processed
generator.print_summary()

# Generate DDL
ddl = generator.generate_ddl(schema, "my_table")

# Get column tuples
columns = generator.get_column_tuples(schema)
```

## Output Examples

### DDL Statement
```sql
CREATE TABLE `your_table` (
  `id` INTEGER,
  `name` STRING,
  `created_at` TIMESTAMP,
  `is_active` BOOLEAN NOT NULL
);
```

### Column Tuples
```python
[
    ('id', 'INTEGER'),
    ('name', 'STRING'),
    ('created_at', 'TIMESTAMP'),
    ('is_active', 'BOOLEAN')
]
```

### Console Output
```
Reading parquet file: data/sample.parquet
Processing 5 columns...

OK id                            -> INTEGER        (NULLABLE)
OK name                          -> STRING         (NULLABLE)
SKIPPING column '_TABLE_META': Column name starts with restricted prefix '_TABLE_'
OK created_at                    -> TIMESTAMP      (NULLABLE)
OK is_active                     -> BOOLEAN        (REQUIRED)

============================================================
SCHEMA GENERATION SUMMARY
============================================================
Valid columns: 4
Skipped columns: 1

Skipped columns details:
   • _TABLE_META: Column name starts with restricted prefix '_TABLE_' (type: string)
```

## Data Type Mappings

| PyArrow/Parquet Type | BigQuery Type |
|---------------------|---------------|
| timestamp           | TIMESTAMP     |
| time                | TIME          |
| date                | DATE          |
| integer types       | INTEGER       |
| floating types      | FLOAT         |
| boolean             | BOOLEAN       |
| string/utf8         | STRING        |
| binary              | BYTES         |
| list/large_list     | ARRAY<T>      |
| struct              | RECORD        |
| unknown types       | STRING        |

## BigQuery Column Restrictions

The tool automatically validates and skips columns that violate BigQuery naming rules:

### Restricted Prefixes (case-insensitive)
- `_PARTITION`
- `_TABLE_`
- `_FILE_`
- `_ROW_TIMESTAMP`
- `__ROOT__`
- `_COLIDENTIFIER`

### Other Restrictions
- Column names must start with a letter or underscore
- Only letters, numbers, and underscores are allowed
- Maximum length of 300 characters
- Cannot be empty

## API Reference

### Main Function

```python
generate_bigquery_schema_from_parquet(
    parquet_file: str, 
    table_name: str = "your_table"
) -> Tuple[str, List[Tuple[str, str]]]
```

**Parameters:**
- `parquet_file`: Path to the Parquet file
- `table_name`: Name for the table in CREATE statement (default: "your_table")

**Returns:**
- Tuple containing CREATE TABLE DDL statement and list of (column_name, datatype) tuples




