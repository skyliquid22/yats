"""Entry point: uv run python -m dashboard

Binds to 127.0.0.1:8787 (localhost only — no keys reach the frontend).
"""
import uvicorn

from dashboard.api import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8787)
