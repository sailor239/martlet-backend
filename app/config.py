import os
import logging
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # points to project-root

DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
CSV_PATH = os.path.join(BASE_DIR, "data", "tiingo_xauusd_5min.csv")

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATE_TIME_FORMAT = f'{DATE_FORMAT} {TIME_FORMAT}'
DATE_TIME_FORMAT_TRADERMADE = '%Y-%m-%d-%H:%M'

TS_FORMAT = {
    "tiingo": DATE_FORMAT,
    "tradermade": DATE_TIME_FORMAT_TRADERMADE
}

LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = '%(asctime)s | %(levelname)s | %(module)s:%(funcName)s:%(lineno)d - %(message)s'
LOGGING_DATE_FORMAT = DATE_TIME_FORMAT
