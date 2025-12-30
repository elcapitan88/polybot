worker: python scripts/enhanced_collector.py --interval 500
web: uvicorn src.api.main:app --host 0.0.0.0 --port ${PORT:-8000}
