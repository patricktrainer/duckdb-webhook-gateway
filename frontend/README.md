# DuckDB Webhook Gateway Frontend

This is the frontend application for the DuckDB Webhook Gateway, providing a user-friendly interface for managing webhooks, reference tables, and user-defined functions.

## Features

- **Dashboard** - View webhook activity, event statistics, and recent events
- **Webhook Management** - Create, edit, and manage webhooks with SQL-based transformations
- **Reference Table Management** - Upload and manage reference tables for data enrichment
- **UDF Management** - Create and manage Python user-defined functions
- **Webhook Testing** - Test webhooks with custom payloads and visualize both raw and transformed data
- **SQL Query Interface** - Run ad-hoc SQL queries against the webhook gateway database

## Getting Started

### Prerequisites

- Node.js 14+ and npm
- DuckDB Webhook Gateway backend running on port 8000

### Installation

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

3. Start the development server:
```bash
npm start
```

The application will be available at `http://localhost:3000`.

## Usage

### Managing Webhooks

1. **Create a Webhook**
   - Click "Register New Webhook" on the Webhooks page
   - Provide a source path, destination URL, and owner
   - Define transformation query using SQL, with `{{payload}}` as a placeholder for the webhook payload
   - Optionally add a filter query to determine which events to forward

2. **Test a Webhook**
   - Go to the Webhook Tester page
   - Select a webhook and provide a test payload
   - Click "Send Test Webhook" to see the test results
   - View detailed results in the tabbed interface:
     - API Response - The initial response from the webhook endpoint
     - Raw Payload - The original webhook payload as received
     - Transformed Data - The payload after SQL transformation is applied
     - Response Details - Delivery status, response codes, and response body

### Managing Reference Tables

1. **Upload a Reference Table**
   - Click "Upload New Table" on the Reference Tables page
   - Select a webhook to associate the table with
   - Choose a CSV file containing your reference data
   - Provide a name and optional description for the table

2. **Using Reference Tables in Transformations**
   - In your webhook transformation queries, join with the reference table using:
   ```sql
   SELECT e.field1, r.field2
   FROM {{payload}} e
   LEFT JOIN ref_<webhook_id>_<table_name> r ON e.key = r.key
   ```

### Creating User-Defined Functions

1. **Register a Python UDF**
   - Click "Create New UDF" on the User-Defined Functions page
   - Select a webhook to associate the UDF with
   - Provide a function name and Python code
   - Follow the UDF guidelines for proper function structure

2. **Using UDFs in Transformations**
   - In your webhook transformation queries, call the UDF using:
   ```sql
   SELECT udf_<webhook_id>_<function_name>(field) AS transformed_field
   FROM {{payload}}
   ```

### Running Ad-hoc Queries

1. **Query the Database**
   - Go to the SQL Query page
   - Write a SQL query or select from example queries
   - Click "Run Query" to execute and view results

## Development

### Project Structure

- `/src/components` - Reusable UI components
- `/src/pages` - Page components for each route
- `/src/api` - API client for backend communication
- `/src/utils` - Utility functions
- `/src/assets` - Static assets

### Adding New Features

1. Create a new component in the appropriate directory
2. Update routing in `App.tsx` if needed
3. Add API functions in `apiClient.ts` if required

## Building for Production

```bash
npm run build
```

This will create an optimized production build in the `build` directory.