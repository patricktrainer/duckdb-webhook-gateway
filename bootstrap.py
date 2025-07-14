#!/usr/bin/env python3
"""
Bootstrap script for the DuckDB Webhook Gateway.
This script demonstrates the application functionality by registering example webhooks
with various transformations and filters.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "http://localhost:8000"
API_KEY = "default_key"
HEADERS = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# Example webhook configurations
EXAMPLE_WEBHOOKS = [
    {
        "source_path": "/github-events",
        "destination_url": "https://httpbin.org/post",
        "transform_query": """
            SELECT 
                repository.name AS repo_name,
                sender.login AS sender,
                type AS event_type,
                created_at,
                CASE 
                    WHEN type = 'PushEvent' THEN 'Code Push'
                    WHEN type = 'PullRequestEvent' THEN 'Pull Request'
                    WHEN type = 'IssuesEvent' THEN 'Issue'
                    ELSE 'Other'
                END AS event_category
            FROM {{payload}}
        """,
        "filter_query": "type IN ('PushEvent', 'PullRequestEvent', 'IssuesEvent')",
        "owner": "demo-team"
    },
    {
        "source_path": "/e-commerce-orders",
        "destination_url": "https://httpbin.org/post",
        "transform_query": """
            SELECT 
                order_id,
                customer.email AS customer_email,
                customer.name AS customer_name,
                total_amount,
                currency,
                status,
                created_at,
                CASE 
                    WHEN total_amount >= 100 THEN 'high_value'
                    WHEN total_amount >= 50 THEN 'medium_value'
                    ELSE 'low_value'
                END AS order_priority
            FROM {{payload}}
        """,
        "filter_query": "status = 'completed' AND total_amount > 0",
        "owner": "ecommerce-team"
    },
    {
        "source_path": "/iot-sensors",
        "destination_url": "https://httpbin.org/post",
        "transform_query": """
            SELECT 
                device_id,
                sensor_type,
                value,
                unit,
                timestamp,
                location.building AS building,
                location.floor AS floor,
                CASE 
                    WHEN sensor_type = 'temperature' AND value > 25 THEN 'alert'
                    WHEN sensor_type = 'humidity' AND value > 80 THEN 'alert'
                    ELSE 'normal'
                END AS status
            FROM {{payload}}
        """,
        "filter_query": "value IS NOT NULL AND timestamp IS NOT NULL",
        "owner": "iot-team"
    },
    {
        "source_path": "/marketing-events",
        "destination_url": "https://httpbin.org/post",
        "transform_query": """
            SELECT 
                event_name,
                user_id,
                campaign_id,
                source,
                medium,
                timestamp,
                properties.revenue AS revenue,
                properties.product_category AS category
            FROM {{payload}}
        """,
        "filter_query": "event_name IN ('purchase', 'signup', 'conversion')",
        "owner": "marketing-team"
    }
]

# Sample test payloads for each webhook
SAMPLE_PAYLOADS = {
    "/github-events": {
        "type": "PushEvent",
        "repository": {
            "name": "my-awesome-repo",
            "full_name": "user/my-awesome-repo"
        },
        "sender": {
            "login": "developer123",
            "id": 12345
        },
        "created_at": "2024-01-15T10:30:00Z",
        "commits": [
            {
                "message": "Add new feature",
                "author": {"name": "Developer", "email": "dev@example.com"}
            }
        ]
    },
    "/e-commerce-orders": {
        "order_id": "ORD-2024-001",
        "customer": {
            "email": "customer@example.com",
            "name": "John Doe",
            "id": "cust_123"
        },
        "total_amount": 149.99,
        "currency": "USD",
        "status": "completed",
        "created_at": "2024-01-15T14:22:00Z",
        "items": [
            {
                "product_id": "prod_456",
                "name": "Wireless Headphones",
                "price": 149.99,
                "quantity": 1
            }
        ]
    },
    "/iot-sensors": {
        "device_id": "sensor_001",
        "sensor_type": "temperature",
        "value": 23.5,
        "unit": "celsius",
        "timestamp": "2024-01-15T16:45:00Z",
        "location": {
            "building": "Building A",
            "floor": 2,
            "room": "Conference Room 201"
        },
        "battery_level": 87
    },
    "/marketing-events": {
        "event_name": "purchase",
        "user_id": "user_789",
        "campaign_id": "campaign_winter2024",
        "source": "google",
        "medium": "cpc",
        "timestamp": "2024-01-15T18:15:00Z",
        "properties": {
            "revenue": 89.99,
            "product_category": "electronics",
            "conversion_path": ["landing_page", "product_page", "checkout"]
        }
    }
}


class WebhookBootstrap:
    """Bootstrap class for setting up example webhooks"""
    
    def __init__(self, base_url: str = BASE_URL, api_key: str = API_KEY):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }
        
    async def check_server_health(self) -> bool:
        """Check if the webhook gateway server is running"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.base_url}/")
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Server health check failed: {e}")
            return False
    
    async def register_webhook(self, webhook_config: Dict) -> Dict:
        """Register a single webhook"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/register",
                    headers=self.headers,
                    json=webhook_config
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Failed to register webhook {webhook_config['source_path']}: {e}")
            raise
    
    async def send_test_webhook(self, path: str, payload: Dict) -> Dict:
        """Send a test webhook to demonstrate functionality"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}{path}",
                    json=payload
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Failed to send test webhook to {path}: {e}")
            raise
    
    async def list_webhooks(self) -> List[Dict]:
        """List all registered webhooks"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/webhooks",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json().get("webhooks", [])
        except Exception as e:
            logger.error(f"Failed to list webhooks: {e}")
            raise
    
    async def bootstrap_examples(self) -> None:
        """Bootstrap the application with example webhooks"""
        logger.info("Starting webhook gateway bootstrap")
        
        # Check server health
        if not await self.check_server_health():
            logger.error("Server is not running. Please start the webhook gateway first.")
            sys.exit(1)
        
        logger.info("✓ Server is running")
        
        # Register example webhooks
        registered_webhooks = []
        for webhook_config in EXAMPLE_WEBHOOKS:
            logger.info(f"Registering webhook: {webhook_config['source_path']}")
            try:
                result = await self.register_webhook(webhook_config)
                registered_webhooks.append(result)
                logger.info(f"✓ Registered webhook: {webhook_config['source_path']}")
            except Exception as e:
                logger.error(f"✗ Failed to register webhook {webhook_config['source_path']}: {e}")
        
        # Send test webhooks to demonstrate functionality
        logger.info("\nSending test webhooks...")
        for path, payload in SAMPLE_PAYLOADS.items():
            try:
                logger.info(f"Sending test webhook to {path}")
                result = await self.send_test_webhook(path, payload)
                logger.info(f"✓ Test webhook sent to {path}, event_id: {result.get('event_id')}")
            except Exception as e:
                logger.error(f"✗ Failed to send test webhook to {path}: {e}")
        
        # List all webhooks
        logger.info("\nRegistered webhooks:")
        try:
            webhooks = await self.list_webhooks()
            for webhook in webhooks:
                logger.info(f"- {webhook['source_path']} -> {webhook['destination_url']} (Owner: {webhook['owner']})")
        except Exception as e:
            logger.error(f"Failed to list webhooks: {e}")
        
        logger.info(f"\n🎉 Bootstrap complete! {len(registered_webhooks)} webhooks registered.")
        logger.info("You can now:")
        logger.info("1. Visit the web UI to explore registered webhooks")
        logger.info("2. Send test webhooks using the webhook tester")
        logger.info("3. Query events using the SQL query interface")
        logger.info("4. Monitor webhook processing and success rates")


def print_usage():
    """Print usage information"""
    print("""
DuckDB Webhook Gateway Bootstrap

This script sets up example webhooks to demonstrate the gateway's functionality.

Usage:
    python bootstrap.py [--url BASE_URL] [--api-key API_KEY]

Examples:
    python bootstrap.py
    python bootstrap.py --url http://localhost:8000 --api-key my_secret_key

The script will:
1. Register example webhooks with various transformations
2. Send test payloads to demonstrate processing
3. Show registered webhooks and their configurations

Example webhooks created:
- /github-events: Process GitHub webhook events
- /e-commerce-orders: Handle e-commerce order data
- /iot-sensors: Process IoT sensor readings
- /marketing-events: Handle marketing analytics events
""")


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bootstrap DuckDB Webhook Gateway with examples")
    parser.add_argument("--url", default=BASE_URL, help="Base URL of the webhook gateway")
    parser.add_argument("--api-key", default=API_KEY, help="API key for authentication")
    parser.add_argument("--help-examples", action="store_true", help="Show usage examples")
    
    args = parser.parse_args()
    
    if args.help_examples:
        print_usage()
        return
    
    # Create bootstrap instance
    bootstrap = WebhookBootstrap(args.url, args.api_key)
    
    # Run bootstrap
    await bootstrap.bootstrap_examples()


if __name__ == "__main__":
    asyncio.run(main())