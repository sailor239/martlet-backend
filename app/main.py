from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.db import init_db_pool, fetch_all_data
from app.db_init import init_db_with_csv
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting lifespan")
    try:
        await init_db_pool()
        print("✅ DB pool initialized")
        await init_db_with_csv()
        print("✅ CSV data loaded")
    except Exception as e:
        print(f"❌ Error during startup: {e}")
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def home():
    html_content = """
    <html>
        <head>
            <title>Martlet Backend Service</title>
        </head>
        <body>
            <h1>Welcome to Martlet Backend Service</h1>
            <p>Use the <a href="/candles">/candles</a> endpoint to see the data.</p>
        </body>
    </html>
    """
    return html_content

@app.get("/candles")
async def get_candle_data():
    return await fetch_all_data()
