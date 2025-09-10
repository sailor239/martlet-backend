from loguru import logger
import pandas as pd
from app.db import db
from datetime import datetime, timezone
from app.utils.data_pipeline_utils import get_hist_price_from_tiingo
from app.utils.date_utils import process_candles


async def sync_forex_data(ticker: str, timeframe: str):
    """Scheduled job to sync forex data from external API"""
    logger.info(f"🔄 Starting forex sync for {ticker} {timeframe}")

    try:
        last_ts = await db.get_last_candle_timestamp(ticker, timeframe)

        if last_ts:
            logger.debug(f"Last candle timestamp for {ticker} {timeframe}: {last_ts}")

            # --- 1. Fetch new raw data from API
            start_date = last_ts.strftime("%Y-%m-%d")
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            raw_data = await get_hist_price_from_tiingo(ticker, timeframe, start_date, end_date)

            if not raw_data:
                logger.warning("⚡ No new records from API")
                return
            
            # --- 2. Normalize into DataFrame
            new_candles = pd.DataFrame([
                {
                    "ticker": ticker,
                    "timeframe": timeframe,
                    "timestamp": datetime.fromisoformat(r["date"].replace("Z", "+00:00")),
                    "open": r["open"],
                    "high": r["high"],
                    "low": r["low"],
                    "close": r["close"],
                }
                for r in raw_data
            ])

            # keep only new / overlapping candles
            new_candles = new_candles[new_candles["timestamp"] >= last_ts]
            if new_candles.empty:
                logger.warning("⚡ No new records to insert after filtering")
                return
            
            # --- 3. Fetch overlap from DB (for recomputation & unconfirmed candles)
            recent_df = await db.get_recent_candles(ticker, timeframe)

            # --- 4. Combine & dedupe (favor new_candles)
            combined_df = pd.concat([recent_df[["ticker", "timeframe", "timestamp", "open", "high", "low", "close"]], new_candles], ignore_index=True)
            combined_df = (
                combined_df
                .drop_duplicates(subset=["ticker", "timeframe", "timestamp"], keep="last")
                .sort_values("timestamp")
                .reset_index(drop=True)
            )

            # --- 5. Recompute indicators
            processed_df = process_candles(combined_df, timeframe)

            # ___ 6. Check if existing records in recent_df are the same as processed_df
            logger.info(recent_df.iloc[:-1].tail(len(processed_df) - 2).reset_index(drop=True).equals(processed_df.iloc[:-2].reset_index(drop=True)))
            # df_1 = recent_df.iloc[:-1].tail(len(processed_df) - 2).reset_index(drop=True)
            # df_2 = processed_df.iloc[:-2].reset_index(drop=True)
            # if not df_1.equals(df_2):
            #     diff = pd.concat([df_1, df_2]).drop_duplicates(keep=False)
            #     logger.warning(diff)
            #     logger.warning("⚠️ Data mismatch detected between existing DB records and recomputed records. This may indicate data inconsistency.")

            # --- 7. Upsert overlap window (recent candles + new ones)
            to_upsert = processed_df[processed_df["timestamp"] >= last_ts]
            processed_records = to_upsert.to_dict(orient="records")

            if processed_records:
                await db.upsert_candles(ticker, timeframe, processed_records)
            else:
                logger.info("⚡ No new records to insert")
            
            logger.info(f"✅ Completed forex sync for {ticker} {timeframe}")

        else:
            logger.error(f"Something went wrong, as there is no existing data for {ticker} {timeframe}. Please check the initial data load process.")
   
    except Exception as e:
        logger.error(f"❌ Forex sync failed for {ticker} {timeframe}: {e}, exc_info=True")
