import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # points to project-root

DATABASE_URL = os.getenv("DATABASE_URL")
CSV_PATH = os.path.join(BASE_DIR, "data", "data.csv")
