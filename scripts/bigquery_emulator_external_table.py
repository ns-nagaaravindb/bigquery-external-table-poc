#!/usr/bin/env python3
"""
BigQuery External Table with Emulator

This script uses the BigQuery emulator from docker-compose to create external tables,
load data, and query the results.

Usage:
    python3 bigquery_emulator_external_table.py <parquet_file_path>
"""

import argparse
import json
import subprocess
import sys
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple

# Import our schema generator
from parquet_to_bq_schema import generate_bigquery_schema_from_parquet


class BigQueryEmulatorExternalTable:
    """Creates external tables using BigQuery emulator."""
    
    def __init__(self, 
                 emulator_host: str = "localhost",
                 emulator_port: int = 9050,
                 file_server_port: int = 8080,
                 project_id: str = "test-project",
                 dataset_id: str = "test_dataset"):
        self.emulator_host = emulator_host
        self.emulator_port = emulator_port
        self.file_server_port = file_server_port
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.base_url = f"http://{emulator_host}:{emulator_port}"
        
    def start_services(self):
        """Start BigQuery emulator and file server using docker-compose."""
        print("Starting BigQuery emulator and file server...")
        
        try:
            # Stop any existing services
            subprocess.run(['docker', 'compose', 'down'], 
                         cwd='../', capture_output=True)
            
            # Start services
            result = subprocess.run(['docker', 'compose', 'up', '-d'], 
                                  cwd='../', capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Failed to start services: {result.stderr}")
                
            print("Services started successfully")
            
            # Wait for services to be ready
            self._wait_for_services()
            
        except Exception as e:
            raise Exception(f"Failed to start services: {e}")
    
    def stop_services(self):
        """Stop BigQuery emulator and file server."""
        print("Stopping services...")
        
        try:
            subprocess.run(['docker', 'compose', 'down'], 
                         cwd='../', capture_output=True)
            print("Services stopped")
        except Exception as e:
            print(f"Warning: Error stopping services: {e}")
    
    def _wait_for_services(self):
        """Wait for BigQuery emulator and file server to be ready."""
        print("Waiting for services to be ready...")
        
        max_retries = 60  # Increased timeout
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Check BigQuery emulator
                bq_response = requests.get(f"{self.base_url}/bigquery/v2/projects")
                
                # Check file server
                file_response = requests.get(f"http://{self.emulator_host}:{self.file_server_port}")
                
                if bq_response.status_code == 200 and file_response.status_code == 200:
                    print("Services are ready!")
                    return
                    
            except requests.exceptions.ConnectionError:
                pass
            
            retry_count += 1
            time.sleep(2)
        
        raise Exception("Services failed to start within timeout period")
    
    def ensure_dataset_exists(self):
        """Ensure the dataset exists in BigQuery emulator."""
        print(f"Ensuring dataset {self.dataset_id} exists...")
        
        # Check if dataset exists
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            print(f"Dataset {self.dataset_id} already exists")
            return
        
        # Create dataset
        create_url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets"
        dataset_data = {
            "datasetReference": {
                "projectId": self.project_id,
                "datasetId": self.dataset_id
            },
            "friendlyName": self.dataset_id,
            "description": "Test dataset for external table demo"
        }
        
        response = requests.post(create_url, json=dataset_data)
        
        if response.status_code == 200:
            print(f"Created dataset {self.dataset_id}")
        else:
            raise Exception(f"Failed to create dataset: {response.status_code} - {response.text}")
    
    def create_regular_table_with_data(self, parquet_file: str, table_name: str) -> Tuple[str, List[Tuple[str, str]]]:
        """Create regular table and load data from parquet file."""
        print(f"Creating regular table {table_name} and loading data from {parquet_file}")
        
        # Generate schema using our function
        try:
            _, column_tuples = generate_bigquery_schema_from_parquet(parquet_file, table_name)
        except Exception as e:
            raise Exception(f"Failed to generate schema: {e}")
        
        # Read parquet data
        import pandas as pd
        try:
            df = pd.read_parquet(parquet_file)
            print(f"Loaded {len(df)} rows from parquet file")
            
            # Filter columns to only include valid ones
            valid_columns = [name for name, _ in column_tuples]
            df_filtered = df[valid_columns]
            print(f"Using {len(valid_columns)} valid columns: {valid_columns}")
            
        except Exception as e:
            raise Exception(f"Failed to read parquet file: {e}")
        
        # Create table schema for BigQuery API
        fields = []
        for name, datatype in column_tuples:
            field = {
                "name": name,
                "type": datatype,
                "mode": "NULLABLE"
            }
            fields.append(field)
        
        # Regular table configuration
        table_data = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": self.dataset_id,
                "tableId": table_name
            },
            "schema": {
                "fields": fields
            },
            "friendlyName": f"Table for {Path(parquet_file).name}",
            "description": f"Table created from {parquet_file}"
        }
        
        # Create the table
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables"
        response = requests.post(url, json=table_data)
        
        if response.status_code == 200:
            print(f"Regular table {table_name} created successfully")
            
            # Load data into the table
            self._load_data_to_table(table_name, df_filtered)
            
            return table_name, column_tuples
        else:
            raise Exception(f"Failed to create regular table: {response.status_code} - {response.text}")
    
    def _load_data_to_table(self, table_name: str, df: 'pd.DataFrame'):
        """Load data from DataFrame into BigQuery table."""
        print(f"Loading {len(df)} rows into table {table_name}...")
        
        # Convert DataFrame to BigQuery insert format
        rows = []
        for _, row in df.iterrows():
            row_data = {}
            for col, value in row.items():
                # Handle NaN/None values
                if pd.isna(value):
                    row_data[col] = None
                else:
                    row_data[col] = str(value)  # Convert all to string for simplicity
            rows.append({"json": row_data})
        
        # Insert data
        insert_data = {
            "rows": rows
        }
        
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables/{table_name}/insertAll"
        response = requests.post(url, json=insert_data)
        
        if response.status_code == 200:
            result = response.json()
            if 'insertErrors' in result:
                print(f"Warning: Insert had errors: {result['insertErrors']}")
            else:
                print(f"Data loaded successfully ({len(rows)} rows)")
        else:
            raise Exception(f"Failed to load data: {response.status_code} - {response.text}")

    def create_external_table(self, parquet_file: str, table_name: str) -> Tuple[str, List[Tuple[str, str]]]:
        """Create external table from parquet file."""
        print(f"Creating external table {table_name} from {parquet_file}")
        
        # Generate schema using our function
        try:
            _, column_tuples = generate_bigquery_schema_from_parquet(parquet_file, table_name)
        except Exception as e:
            raise Exception(f"Failed to generate schema: {e}")
        
        # Build the external table definition
        parquet_filename = Path(parquet_file).name
        file_server_url = f"http://{self.emulator_host}:{self.file_server_port}/data/{parquet_filename}"
        
        # Debug: Check if file is accessible
        print(f"Checking file accessibility at: {file_server_url}")
        try:
            file_check = requests.head(file_server_url)
            if file_check.status_code == 200:
                print(f"File is accessible (size: {file_check.headers.get('content-length', 'unknown')} bytes)")
            else:
                print(f"Warning: File check returned status {file_check.status_code}")
        except Exception as e:
            print(f"Warning: Could not check file accessibility: {e}")
        
        # Create table schema for BigQuery API
        fields = []
        for name, datatype in column_tuples:
            field = {
                "name": name,
                "type": datatype,
                "mode": "NULLABLE"
            }
            fields.append(field)
        
        # External table configuration
        table_data = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": self.dataset_id,
                "tableId": table_name
            },
            "schema": {
                "fields": fields
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [file_server_url],
                "autodetect": False
            },
            "friendlyName": f"External table for {parquet_filename}",
            "description": f"External table created from {parquet_file}"
        }
        
        # Create the table
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables"
        response = requests.post(url, json=table_data)
        
        if response.status_code == 200:
            print(f"External table {table_name} created successfully")
            return table_name, column_tuples
        else:
            raise Exception(f"Failed to create external table: {response.status_code} - {response.text}")
    
    def query_table(self, table_name: str, limit: int = 10) -> Dict:
        """Query the external table and return results."""
        print(f"Querying table {table_name}...")
        
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        LIMIT {limit}
        """
        
        query_data = {
            "query": query,
            "useLegacySql": False,
            "location": "US"
        }
        
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/queries"
        response = requests.post(url, json=query_data)
        
        if response.status_code == 200:
            result = response.json()
            print(f"Query successful! Retrieved {len(result.get('rows', []))} rows")
            return result
        else:
            raise Exception(f"Query failed: {response.status_code} - {response.text}")
    
    def get_table_info(self, table_name: str) -> Dict:
        """Get information about the table."""
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables/{table_name}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get table info: {response.status_code} - {response.text}")
    
    def cleanup_table(self, table_name: str):
        """Delete the external table."""
        print(f"Cleaning up table {table_name}...")
        
        url = f"{self.base_url}/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables/{table_name}"
        response = requests.delete(url)
        
        if response.status_code == 204:
            print(f"Table {table_name} deleted successfully")
        else:
            print(f"Warning: Failed to delete table: {response.status_code} - {response.text}")


def print_query_results(result: Dict):
    """Print query results in a formatted way."""
    if 'rows' not in result or not result['rows']:
        print("No data returned from query")
        return
    
    # Get column names
    schema = result.get('schema', {}).get('fields', [])
    column_names = [field['name'] for field in schema]
    
    print()
    print("QUERY RESULTS:")
    print("=" * 60)
    
    # Print header
    header = " | ".join(f"{name:<15}" for name in column_names)
    print(header)
    print("-" * len(header))
    
    # Print rows
    for row in result['rows']:
        values = []
        for cell in row.get('f', []):
            value = cell.get('v', 'NULL')
            if value is None:
                value = 'NULL'
            values.append(f"{str(value):<15}")
        print(" | ".join(values))
    
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Create external table in BigQuery emulator and query data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 bigquery_emulator_external_table.py ../data/sample_data_clean.parquet
    python3 bigquery_emulator_external_table.py ../data/sample_data_problematic.parquet --table my_data
        """
    )
    
    parser.add_argument('parquet_file', help='Path to the parquet file')
    parser.add_argument('--table', '-t', default='external_data', help='Name of the table to create')
    parser.add_argument('--limit', '-l', type=int, default=10, help='Number of rows to query')
    parser.add_argument('--keep-running', action='store_true', help='Keep services running after completion')
    parser.add_argument('--use-regular-table', action='store_true', help='Create regular table instead of external table')
    
    args = parser.parse_args()
    
    # Check if parquet file exists
    parquet_path = Path(args.parquet_file)
    if not parquet_path.exists():
        print(f"ERROR: Parquet file not found: {args.parquet_file}")
        sys.exit(1)
    
    # Initialize the external table manager
    emulator = BigQueryEmulatorExternalTable()
    
    try:
        print("BIGQUERY EXTERNAL TABLE DEMO")
        print("=" * 60)
        
        # Start services
        emulator.start_services()
        
        # Ensure dataset exists
        emulator.ensure_dataset_exists()
        
        # Create table (external or regular based on argument)
        if args.use_regular_table:
            print("Creating regular table with data loading...")
            table_name, column_tuples = emulator.create_regular_table_with_data(args.parquet_file, args.table)
        else:
            print("Creating external table...")
            table_name, column_tuples = emulator.create_external_table(args.parquet_file, args.table)
            
            # If external table returns no data, try regular table
            result = emulator.query_table(table_name, 1)  # Just check if we get any data
            if not result.get('rows'):
                print()
                print("External table returned no data. Trying regular table approach...")
                # Delete the external table first
                emulator.cleanup_table(table_name)
                # Create regular table
                table_name, column_tuples = emulator.create_regular_table_with_data(args.parquet_file, args.table)
        
        # Get table information
        table_info = emulator.get_table_info(table_name)
        print()
        print("TABLE INFORMATION:")
        print("-" * 40)
        print(f"Table ID: {table_info['tableReference']['tableId']}")
        print(f"Project: {table_info['tableReference']['projectId']}")
        print(f"Dataset: {table_info['tableReference']['datasetId']}")
        print(f"Type: {table_info['type']}")
        print(f"Columns: {len(column_tuples)}")
        
        # Query the table
        result = emulator.query_table(table_name, args.limit)
        print_query_results(result)
        
        # Show column information
        print("COLUMN SCHEMA:")
        print("-" * 40)
        for i, (name, datatype) in enumerate(column_tuples, 1):
            print(f"{i:2d}. {name:<25} -> {datatype}")
        
        print()
        print("SUCCESS: External table created and queried successfully!")
        
        if not args.keep_running:
            # Cleanup
            emulator.cleanup_table(table_name)
            emulator.stop_services()
        else:
            print()
            print("Services are still running. You can manually query the table:")
            print(f"Table: {emulator.project_id}.{emulator.dataset_id}.{table_name}")
            print(f"BigQuery endpoint: http://{emulator.emulator_host}:{emulator.emulator_port}")
            print("To stop services: docker compose down")
        
    except Exception as e:
        print(f"ERROR: {e}")
        if not args.keep_running:
            emulator.stop_services()
        sys.exit(1)


if __name__ == "__main__":
    main()