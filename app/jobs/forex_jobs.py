import logging
from app.db import db
from datetime import datetime, timezone
from app.utils.data_pipeline_utils import get_hist_price_from_tiingo

logger = logging.getLogger(__name__)

async def sync_forex_data(ticker: str, timeframe: str):
    """Scheduled job to sync forex data from external API"""
    try:
        logger.info(f"🔄 Starting forex sync for {ticker} {timeframe}")
        
        last_ts = await db.get_last_candle_timestamp(ticker, timeframe)
        if last_ts:
            logger.info(f"Last candle timestamp for {ticker} {timeframe}: {last_ts}")
            start_date = last_ts.strftime("%Y-%m-%d")
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            raw_data = await get_hist_price_from_tiingo(ticker, timeframe, start_date, end_date)
            if raw_data:
                processed_records = []
                for record in raw_data:
                    processed_record = {
                        'timestamp': datetime.fromisoformat(record['date'].replace('Z', '+00:00')),
                        'open': record['open'],
                        'high': record['high'], 
                        'low': record['low'],
                        'close': record['close'],
                    }
                    processed_records.append(processed_record)
                
                processed_records = [r for r in processed_records if r['timestamp'] >= last_ts]
                if processed_records:
                    await db.upsert_candles(ticker, timeframe, processed_records)
                else:
                    logging.info("⚡ No new records to insert")
        else:
            logger.info(f"Something went wrong, as there is no existing data for {ticker} {timeframe}. Please check the initial data load process.")
        
        logger.info(f"✅ Completed forex sync for {ticker} {timeframe}")
        
    except Exception as e:
        logger.error(f"❌ Forex sync failed for {ticker}: {e}, exc_info=True")
