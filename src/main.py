"""
Main application file for the webhook gateway.
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Depends
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from .api.dependencies import initialize_database, get_webhook_service, get_webhook_processing_service, get_event_service
from .api.routes import (
    webhook_routes,
    query_routes,
    event_routes,
    reference_routes,
    udf_routes,
    stats_routes
)
from .core.config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup and shutdown events"""
    # Startup
    logger.info("Webhook Gateway starting up")

    # Ensure directories exist
    config.ensure_directories()

    # Log environment information
    if config.FRONTEND_DIR.exists() and config.FRONTEND_DIR.is_dir():
        logger.info(f"Frontend build found at {config.FRONTEND_DIR.absolute()}")
    else:
        logger.warning(f"Frontend build not found at {config.FRONTEND_DIR.absolute()}")

    # Check database file
    db_path = Path(config.DUCKDB_PATH)
    if db_path.exists():
        logger.info(f"Database found at {db_path.absolute()}")
    else:
        logger.info(f"Creating new database at {db_path.absolute()}")

    # Initialize database
    logger.info("Initializing database connection")
    await initialize_database()
    
    yield

    # Shutdown
    logger.info("Webhook Gateway shutting down")


# Create the FastAPI app
app = FastAPI(title="Webhook Gateway", lifespan=lifespan)

# Include all route modules at root level (original API structure)
app.include_router(webhook_routes.router, prefix="")
app.include_router(query_routes.router, prefix="")
app.include_router(event_routes.router, prefix="")
app.include_router(reference_routes.router, prefix="")
app.include_router(udf_routes.router, prefix="")
app.include_router(stats_routes.router, prefix="")

# Also include with /api prefix for future API versioning
app.include_router(webhook_routes.router, prefix="/api", tags=["api-v1"])
app.include_router(query_routes.router, prefix="/api", tags=["api-v1"])
app.include_router(event_routes.router, prefix="/api", tags=["api-v1"])
app.include_router(reference_routes.router, prefix="/api", tags=["api-v1"])
app.include_router(udf_routes.router, prefix="/api", tags=["api-v1"])
app.include_router(stats_routes.router, prefix="/api", tags=["api-v1"])


# Static file handlers
@app.get("/favicon.ico")
async def get_favicon():
    """Return a favicon for the browser"""
    favicon_path = config.STATIC_DIR / "favicon.ico"
    if favicon_path.exists():
        return FileResponse(favicon_path)
    return Response(status_code=204)


@app.get("/logo192.png")
async def get_logo():
    """Return a logo image for the browser"""
    logo_path = config.STATIC_DIR / "logo192.png"
    if logo_path.exists():
        return FileResponse(logo_path)
    return Response(status_code=204)


@app.get("/service-worker.js")
async def get_service_worker():
    """Return a service worker file"""
    sw_path = config.STATIC_DIR / "service-worker.js"
    if sw_path.exists():
        return FileResponse(sw_path)
    return Response(status_code=204)


# Mount static files
app.mount("/static", StaticFiles(directory=str(config.STATIC_DIR)), name="static")


# Root endpoint for healthcheck
@app.get("/")
async def root():
    """Root endpoint for health checks"""
    return {"status": "ok", "message": "DuckDB Webhook Gateway is running"}


# Webhook handling route - must be after API routes but before SPA catch-all
@app.post("/{path:path}")
async def handle_webhook(
    path: str,
    request: Request,
    background_tasks: BackgroundTasks,
    webhook_service = Depends(get_webhook_service),
    webhook_processing_service = Depends(get_webhook_processing_service),
    event_service = Depends(get_event_service)
):
    """Handle an incoming webhook request"""
    from .models.responses import WebhookAcceptedResponse
    
    # Ensure path starts with a slash
    if not path.startswith('/'):
        path = f'/{path}'
    
    # Get webhook configuration
    webhook = await webhook_service.get_webhook_by_path(path)
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    # Parse JSON payload
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {str(e)}")
    
    # Log the raw event
    raw_event_id = await event_service.log_raw_event(path, payload)
    
    # Process the webhook in the background
    background_tasks.add_task(
        webhook_processing_service.process_webhook,
        webhook,
        raw_event_id,
        payload
    )
    
    return WebhookAcceptedResponse(status="accepted", event_id=raw_event_id)


# Catch-all route for SPA - must be last
@app.get("/{full_path:path}", include_in_schema=False)
async def serve_spa(full_path: str):
    """Serve the Single Page Application for any routes not matched by API endpoints"""
    # For frontend routes, serve the index.html
    spa_index = config.FRONTEND_DIR / "index.html"
    if spa_index.exists():
        return FileResponse(spa_index)

    # If no frontend build exists, return a helpful message
    return {"message": "Frontend not found. Please run 'npm run build' in the frontend directory."}


def main():
    """Main entry point"""
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the FastAPI app
    uvicorn.run(app, host=config.HOST, port=config.PORT)


if __name__ == "__main__":
    main()