import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
        self._setup_jobs()
    
    def _setup_jobs(self):
        """Configure all scheduled jobs"""
        from app.jobs.forex_jobs import sync_forex_data
        
        self.scheduler.add_job(
            sync_forex_data,
            CronTrigger(minute="*/5"),
            kwargs={
                "ticker": "xauusd",
                "timeframe": "5min",
            },
            id="sync_xauusd"
        )
    
    def start(self):
        """Start the scheduler"""
        if not self.is_running:
            self.scheduler.start()
            self.is_running = True
            logger.info("✅ Scheduler started successfully")
        else:
            logger.warning("Scheduler is already running")
    
    def stop(self):
        """Stop the scheduler"""
        if self.is_running:
            self.scheduler.shutdown(wait=False)
            self.is_running = False
            logger.info("✅ Scheduler stopped successfully")
        else:
            logger.warning("Scheduler is not running")
    
    def get_jobs(self):
        """Get list of all scheduled jobs"""
        return self.scheduler.get_jobs()
    
    def get_job_info(self):
        """Get formatted job information"""
        jobs = self.get_jobs()
        job_info = []
        for job in jobs:
            job_info.append({
                "id": job.id,
                "name": job.name or job.func.__name__,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            })
        return job_info


scheduler_service = SchedulerService()
