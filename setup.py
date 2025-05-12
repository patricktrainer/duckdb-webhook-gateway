#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="duckdb-webhook-gateway",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "duckdb>=0.8.1",
        "fastapi>=0.95.0",
        "uvicorn>=0.22.0",
        "httpx>=0.24.0",
        "pandas>=2.0.0",
        "pydantic>=1.10.7",
    ],
    python_requires=">=3.7",
)