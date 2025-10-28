#!/usr/bin/env python3
"""
Parquet to BigQuery Schema Generator

This script reads a parquet file and generates a BigQuery schema definition,
automatically skipping columns that conflict with BigQuery restricted prefixes.

Usage:
    python3 parquet_to_bq_schema.py <parquet_file_path>
"""

from typing import Dict, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


class BigQuerySchemaGenerator:
    """Generate BigQuery schema from Parquet files with column validation."""

    def __init__(self):
        self.skipped_columns = []
        self.valid_columns = []

    def is_valid_column_name(self, column_name: str) -> Tuple[bool, str]:
        """
        Check if a column name is valid for BigQuery.
        
        Returns:
            Tuple[bool, str]: (is_valid, reason_if_invalid)
        """
        # Check if empty
        if not column_name:
            return False, "Empty column name"

        # Check length (max 300 characters)
        if len(column_name) > 300:
            return False, f"Column name too long ({len(column_name)} > 300 chars)"

        # Check for BigQuery specific restricted prefixes (case-insensitive)
        column_name_upper = column_name.upper()
        restricted_prefixes = [
            '_PARTITION',
            '_TABLE_',
            '_FILE_',
            '_ROW_TIMESTAMP',
            '__ROOT__',
            '_COLIDENTIFIER'
        ]

        for prefix in restricted_prefixes:
            if column_name_upper.startswith(prefix):
                return False, f"Column name starts with restricted prefix '{prefix}'"

        # Check if starts with letter or underscore
        if not (column_name[0].isalpha() or column_name[0] == '_'):
            return False, "Column name must start with letter or underscore"

        # Check if contains only valid characters (letters, numbers, underscores)
        if not all(c.isalnum() or c == '_' for c in column_name):
            return False, "Column name contains invalid characters"

        return True, ""

    def get_bigquery_type(self, arrow_type) -> str:
        """Convert PyArrow type to BigQuery type."""
        # Handle timestamp types with different units
        if pa.types.is_timestamp(arrow_type):
            return 'TIMESTAMP'
        elif pa.types.is_time(arrow_type):
            return 'TIME'
        elif pa.types.is_date(arrow_type):
            return 'DATE'
        elif pa.types.is_integer(arrow_type):
            return 'INTEGER'
        elif pa.types.is_floating(arrow_type):
            return 'FLOAT'
        elif pa.types.is_boolean(arrow_type):
            return 'BOOLEAN'
        elif pa.types.is_string(arrow_type) or pa.types.is_unicode(arrow_type):
            return 'STRING'
        elif pa.types.is_binary(arrow_type):
            return 'BYTES'
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            # For arrays, we need to determine the element type
            element_type = self.get_bigquery_type(arrow_type.value_type)
            return f'ARRAY<{element_type}>'
        elif pa.types.is_struct(arrow_type):
            return 'RECORD'
        else:
            # Default to STRING for unknown types
            return 'STRING'

    def read_parquet_schema(self, parquet_file: str) -> pa.Schema:
        """Read schema from parquet file."""
        try:
            table = pq.read_table(parquet_file)
            return table.schema
        except Exception as e:
            raise Exception(f"Error reading parquet file {parquet_file}: {e}")

    def generate_schema(self, parquet_file: str) -> List[Dict]:
        """
        Generate BigQuery schema from parquet file.
        
        Returns:
            List of BigQuery field definitions
        """
        print(f"Reading parquet file: {parquet_file}")

        # Read parquet schema
        arrow_schema = self.read_parquet_schema(parquet_file)

        # Reset tracking lists
        self.skipped_columns = []
        self.valid_columns = []

        bq_schema = []

        print(f"Processing {len(arrow_schema.names)} columns...")
        print()

        for field in arrow_schema:
            column_name = field.name
            arrow_type = field.type

            # Check if column name is valid
            is_valid, reason = self.is_valid_column_name(column_name)

            if not is_valid:
                print(f"SKIPPING column '{column_name}': {reason}")
                self.skipped_columns.append({
                    'name': column_name,
                    'reason': reason,
                    'type': str(arrow_type)
                })
                continue

            # Convert to BigQuery type
            bq_type = self.get_bigquery_type(arrow_type)

            # Determine mode (nullable vs required)
            mode = 'NULLABLE' if field.nullable else 'REQUIRED'

            field_def = {
                'name': column_name,
                'type': bq_type,
                'mode': mode
            }

            bq_schema.append(field_def)
            self.valid_columns.append(column_name)

            print(f"OK {column_name:<30} -> {bq_type:<15} ({mode})")

        return bq_schema

    def print_summary(self):
        """Print summary of schema generation."""
        print()
        print("=" * 60)
        print("SCHEMA GENERATION SUMMARY")
        print("=" * 60)
        print(f"Valid columns: {len(self.valid_columns)}")
        print(f"Skipped columns: {len(self.skipped_columns)}")
        print()

        if self.skipped_columns:
            print("Skipped columns details:")
            for col in self.skipped_columns:
                print(f"   • {col['name']}: {col['reason']} (type: {col['type']})")
            print()

    def get_column_tuples(self, schema: List[Dict]) -> List[Tuple[str, str]]:
        """
        Get list of tuples containing (column_name, bigquery_datatype).
        
        Returns:
            List of tuples: [(column_name, datatype), ...]
        """
        return [(field['name'], field['type']) for field in schema]

    def generate_ddl(self, schema: List[Dict], table_name: str = "your_table") -> str:
        """Generate CREATE TABLE DDL statement."""
        ddl_lines = [f"CREATE TABLE `{table_name}` ("]

        field_definitions = []
        for field in schema:
            field_def = f"  `{field['name']}` {field['type']}"
            if field['mode'] == 'REQUIRED':
                field_def += " NOT NULL"
            field_definitions.append(field_def)

        ddl_lines.append(",\n".join(field_definitions))
        ddl_lines.append(");")

        return "\n".join(ddl_lines)


def generate_bigquery_schema_from_parquet(parquet_file: str, table_name: str = "your_table") -> Tuple[
    str, List[Tuple[str, str]]]:
    """
    Generate BigQuery schema from parquet file and return CREATE TABLE statement and column tuples.
    
    Args:
        parquet_file: Path to the parquet file
        table_name: Name for the table in CREATE statement
        
    Returns:
        Tuple containing:
        - CREATE TABLE statement as string
        - List of tuples with (column_name, datatype)
    """
    generator = BigQuerySchemaGenerator()

    try:
        schema = generator.generate_schema(parquet_file)
        generator.print_summary()

        if not schema:
            raise Exception("No valid columns found! All columns were skipped.")

        # Generate CREATE TABLE DDL
        ddl_statement = generator.generate_ddl(schema, table_name)

        # Get column tuples
        column_tuples = generator.get_column_tuples(schema)

        return ddl_statement, column_tuples

    except Exception as e:
        raise Exception(f"Error generating schema: {e}")


if __name__ == "__main__":
    ddl_statement, column_tuples = generate_bigquery_schema_from_parquet(
        # "/Users/nagaaravindb/rs/bigquery-external-table-poc/data/sample_data_problematic.parquet"
        "../data/nspolicy.parquet"
    )

    # Print the return values
    print()
    print("CREATE TABLE DDL:")
    print("-" * 60)
    print(ddl_statement)

    print()
    print("COLUMN TUPLES (name, datatype):")
    print("-" * 60)
    for i, (name, datatype) in enumerate(column_tuples, 1):
        print(f"{i:2d}. ('{name}', '{datatype}')")
