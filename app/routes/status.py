from fastapi import APIRouter
from app.db import db
from app.services.scheduler import scheduler_service

router = APIRouter(prefix="/status", tags=["Status"])

@router.get("/")
async def get_status():
    try:
        async with db.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {e}"

    return {
        "database": db_status,
        "scheduler": "running" if scheduler_service.is_running else "stopped",
        "jobs": len(scheduler_service.get_jobs())
    }
