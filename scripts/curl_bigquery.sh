#!/bin/bash
# Simple Working Curl Commands for BigQuery Emulator
# Usage: ./curl_bigquery.sh

echo "🔍 BigQuery Emulator Curl Commands"
echo "==================================="
echo "Emulator: http://localhost:9050"
echo "Project: test-project"
echo "Dataset: test_dataset"
echo "Table: test_table"
echo ""

echo "# List projects:"
echo "curl -s \"http://localhost:9050/bigquery/v2/projects\""
echo ""
echo "# List tables:"
echo "curl -s \"http://localhost:9050/bigquery/v2/projects/test-project/datasets/test_dataset/tables\""
echo ""
echo "# Query data (limit 3):"
echo "curl -X POST -H \"Content-Type: application/json\" -d '{\"query\": \"SELECT * FROM \`test-project.test_dataset.test_table\` LIMIT 3\", \"useLegacySql\": false}' \"http://localhost:9050/bigquery/v2/projects/test-project/queries\""
echo ""
echo "# Count rows:"
echo "curl -X POST -H \"Content-Type: application/json\" -d '{\"query\": \"SELECT COUNT(*) FROM \`test-project.test_dataset.test_table\`\", \"useLegacySql\": false}' \"http://localhost:9050/bigquery/v2/projects/test-project/queries\""
echo ""
echo "# Get table info:"
echo "curl -s \"http://localhost:9050/bigquery/v2/projects/test-project/datasets/test_dataset/tables/test_table\""
echo ""
