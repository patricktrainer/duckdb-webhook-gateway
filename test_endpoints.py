#!/usr/bin/env python3
"""
Test script to verify that the DuckDB Webhook Gateway API endpoints are working correctly.
This script tests the key endpoints to make sure they're responding properly.
"""

import requests
import json
import sys
from pprint import pprint

BASE_URL = "http://localhost:8000"
API_KEY = "default_key"  # Default API key

def test_endpoint(method, endpoint, data=None, expected_status=200):
    """Test an API endpoint and print the result"""
    url = f"{BASE_URL}{endpoint}"
    headers = {"X-API-Key": API_KEY}
    
    print(f"\n=== Testing {method} {endpoint} ===")
    
    try:
        if method.lower() == "get":
            response = requests.get(url, headers=headers)
        elif method.lower() == "post":
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=data)
        else:
            print(f"Unsupported method: {method}")
            return False
        
        print(f"Status code: {response.status_code}")
        
        if response.status_code != expected_status:
            print(f"❌ Expected status {expected_status}, but got {response.status_code}")
            print(f"Response: {response.text}")
            return False
        
        try:
            json_response = response.json()
            print("Response:")
            pprint(json_response)
        except:
            print(f"Response is not JSON: {response.text[:100]}...")
        
        print("✅ Test passed")
        return True
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Run tests for all key endpoints"""
    success = True
    
    # Test stats endpoint
    success = test_endpoint("GET", "/stats") and success
    
    # Test webhooks endpoint
    success = test_endpoint("GET", "/webhooks") and success
    
    # Test events endpoint
    success = test_endpoint("GET", "/events") and success
    
    # Test register webhook endpoint
    webhook_data = {
        "source_path": "/test-webhook",
        "destination_url": "https://httpbin.org/post",
        "transform_query": "SELECT * FROM {{payload}}",
        "owner": "test-user"
    }
    success = test_endpoint("POST", "/register", webhook_data) and success
    
    # Test execute query endpoint (this one uses form data, so we'll skip for now)
    
    if success:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())