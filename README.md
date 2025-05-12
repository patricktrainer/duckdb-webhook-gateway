# DuckDB Webhook Gateway

A powerful webhook processing system using DuckDB as both a storage mechanism and a computational engine.

## Overview

DuckDB Webhook Gateway is a flexible, high-performance system for processing, transforming, and routing webhook events. Unlike traditional webhook handlers that require custom code for each integration, this gateway uses SQL as a universal interface for data transformation and filtering.

DuckDB serves as both the storage layer and the computational engine, enabling complex data operations directly on incoming webhook payloads without requiring intermediate ETL processes.

### Key Features

- **Dynamic webhook registration** with SQL-defined transformations and filtering
- **Webhook-specific reference tables** for enriching webhook data with lookups
- **Runtime-registered Python UDFs** for custom transformations beyond SQL
- **Ad-hoc SQL query capabilities** for analytics, debugging, and auditing
- **Thread-safe DuckDB operations** for reliable concurrent processing
- **Built-in audit trail** of all webhook events (raw and transformed)
- **Interactive webhook testing** with visualization of transformed data

![webhookui](etc/duckdb-webhook-ui.gif)

## Real-World Use Cases

### 1. DevOps Event Router

Route GitHub or GitLab events to different services based on content:
- Send PR events to code review tools
- Route issues with security tags to security teams
- Trigger CI/CD pipelines for specific branch events
- Extract JIRA keys from commit messages to update tickets

### 2. E-commerce Order Processing

Process incoming orders from multiple platforms:
- Transform order payloads from different sources into a consistent format
- Enrich orders with customer data from reference tables
- Apply business rules via SQL filters (e.g., fraud detection)
- Route high-value orders to priority fulfillment

### 3. IoT Data Processing

Manage streams of IoT device data:
- Filter out readings below sensor thresholds
- Transform raw sensor data into actionable metrics
- Enrich events with device metadata from reference tables
- Trigger alerts based on anomaly detection

### 4. Marketing Automation

Process webhook events from marketing platforms:
- Transform campaign performance data into standardized formats
- Join events with customer segments from reference tables
- Filter for high-value conversion events
- Route customer actions to appropriate teams based on behavior

### 5. Financial Transaction Processing

Handle payment webhook events with precision:
- Transform transaction data from payment processors
- Apply complex compliance and validation rules
- Enrich transactions with account metadata
- Route suspicious transactions for manual review

## Installation and Setup

### Prerequisites
- Python 3.8+
- pip

### Standard Installation
1. Clone the repository:
```bash
git clone https://github.com/patricktrainer/duckdb-webhook-gateway.git
cd duckdb-webhook-gateway
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package in development mode:
```bash
pip install -e .
```

4. Run the application:
```bash
python -m src.app
```

The server will start on `http://localhost:8000`.

### Docker Installation

The application can be deployed using Docker for easier setup and deployment.

#### Prerequisites
- Docker
- Docker Compose

#### Steps to Run with Docker

1. Clone the repository:
```bash
git clone https://github.com/patricktrainer/duckdb-webhook-gateway.git
cd duckdb-webhook-gateway
```

2. Configure environment (optional):
   Edit the `docker-compose.yml` file to set your preferred API key and other configurations.

3. Build and start the containers:
```bash
docker-compose up -d
```

4. Access the application:
   - Frontend: http://localhost:80
   - Backend API: http://localhost:8000

#### Docker Environment Variables

You can customize the Docker deployment by editing the environment variables in `docker-compose.yml`:

- `WEBHOOK_GATEWAY_API_KEY`: API key for authenticating with the webhook gateway

#### Data Persistence

The Docker setup uses a named volume (`webhook-data`) to persist the DuckDB database file across container restarts. You can manage this volume using standard Docker commands:

```bash
# List all volumes
docker volume ls

# Inspect the webhook data volume
docker volume inspect duckdb-webhook-gateway_webhook-data

# Backup the volume data
docker run --rm -v duckdb-webhook-gateway_webhook-data:/source -v $(pwd):/backup alpine tar -czvf /backup/webhook-data-backup.tar.gz -C /source .
```

## How It Works

1. **Webhook Registration**: Define source paths, destination URLs, and SQL transformations.
2. **Event Reception**: The gateway receives webhook events at the registered paths.
3. **Data Transformation**: DuckDB applies the SQL transformations to the incoming JSON.
4. **Filtering**: Optional SQL filter clauses determine which events to forward.
5. **Enrichment**: Reference tables and custom UDFs can be used to enrich the data.
6. **Delivery**: Transformed events are forwarded to destination endpoints.
7. **Auditing**: All raw and transformed events are stored for analysis and replay.
8. **Testing & Visualization**: Built-in webhook tester allows viewing both raw and transformed data.

## Running Tests

The project includes a comprehensive test suite covering all aspects of the application:

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test modules
pytest tests/test_db_manager.py

# Run unit tests without integration tests
pytest -k "not integration"

# Run only integration tests
pytest -k "integration"
```

## Example Usage

Here are some examples of how to use the DuckDB Webhook Gateway:

### 1. Registering a webhook

```bash
curl -X POST http://localhost:8000/register \
  -H "X-API-Key: default_key" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/github-events",
    "destination_url": "https://example.com/webhook-handler",
    "transform_query": "SELECT repository.name AS repo_name, sender.login AS sender, type AS event_type FROM {{payload}}",
    "filter_query": "type IN (\'PushEvent\', \'PullRequestEvent\')",
    "owner": "team-a"
  }'
```

### 2. Uploading a reference table

```bash
# Create a users.csv file:
# user_id,username,department,role
# 1,john_doe,engineering,developer
# 2,jane_smith,product,manager
# 3,bob_jones,engineering,devops

curl -X POST http://localhost:8000/upload_table \
  -H "X-API-Key: default_key" \
  -F "webhook_id=<webhook_id>" \
  -F "table_name=users" \
  -F "description=User information for enriching webhook data" \
  -F "file=@users.csv"
```

### 3. Registering a Python UDF

```bash
curl -X POST http://localhost:8000/register_udf \
  -H "X-API-Key: default_key" \
  -F "webhook_id=a2b392f3-8cf7-43d5-936c-322d64c9f07e" \
  -F "function_name=extract_jira_key" \
  -F 'function_code=def extract_jira_key(text: str) -> str:
    """Extract JIRA issue keys from text"""
    import re
    if not text:
        return None
    match = re.search(r"[A-Z]+-\d+", text)
    return match.group(0) if match else None'
```

### 4. Testing Webhooks via the UI

The gateway includes a built-in webhook testing UI that allows you to:

1. Select any registered webhook
2. Craft custom JSON payloads
3. Send test webhooks
4. View complete processing results including:
   - Original API response
   - Raw payload as received
   - Transformed data after SQL processing
   - Delivery status and response details

To access the webhook tester:

1. Open the web UI at `http://localhost:80` (or `http://localhost:8000` if using direct Python install)
2. Navigate to the "Webhook Tester" section
3. Select a webhook and customize your payload
4. View the processed results in the tabbed interface

### 5. Example admin queries

```bash
# Get all events for a specific source path
curl -X POST http://localhost:8000/query \
  -H "X-API-Key: default_key" \
  -F 'query=SELECT r.id, r.timestamp, r.source_path, r.payload, t.success, t.response_code FROM raw_events r LEFT JOIN transformed_events t ON r.id = t.raw_event_id WHERE r.source_path = "/github-events" ORDER BY r.timestamp DESC LIMIT 10'

# Get success rate by webhook
curl -X POST http://localhost:8000/query \
  -H "X-API-Key: default_key" \
  -F 'query=SELECT w.source_path, COUNT(t.id) as total, SUM(CASE WHEN t.success THEN 1 ELSE 0 END) as success_count, CAST(SUM(CASE WHEN t.success THEN 1 ELSE 0 END) AS FLOAT) / COUNT(t.id) as success_rate FROM webhooks w JOIN transformed_events t ON w.id = t.webhook_id GROUP BY w.source_path'
```

## System Architecture

The system is built around DuckDB's unique capabilities:

1. **JSON Processing**: Uses DuckDB's JSON functions to query directly against webhook payloads
2. **In-Memory Processing**: Leverages DuckDB's high-performance query engine
3. **Temporary Views**: Creates temporary views of payload data for transformation
4. **User-Defined Functions**: Extends SQL capabilities with custom Python functions
5. **Thread Safety**: Manages concurrent operations with query locks

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License Version 2.0 - see the LICENSE file for details.