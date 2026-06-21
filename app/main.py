import os
import time
import logging
import json
from fastapi import FastAPI, HTTPException
import redis
import psycopg2
from psycopg2.extras import RealDictCursor

# Ensure log directory exists
os.makedirs("/var/log/app", exist_ok=True)

# Configure logging to write structural text files to the shared volume
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/var/log/app/service.log"),
        logging.StreamHandler()
    ]
)

app = FastAPI(title="Jarvis Target Stack")

# Initialize clients
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database="inventory_db",
        user="admin",
        password="secretpassword"
    )

@app.get("/service/{service_name}")
def get_service_status(service_name: str, force_error: bool = False):
    logging.info(f"Received request for service status: {service_name}")
    
    # 1. Simulated Error injection point for testing Jarvis
    if force_error:
        logging.error(f"CRITICAL: Failed connection handshake to database storage layer for service: {service_name}")
        raise HTTPException(status_code=500, detail="Internal Database Error Connection Timeout")

    # 2. Check Redis Cache Layer
    try:
        cached_data = redis_client.get(service_name)
        if cached_data:
            logging.info(f"CACHE HIT: Retrieved metrics for {service_name} from Redis.")
            return {"source": "cache", "data": json.loads(cached_data)}
    except redis.RedisError as re:
        logging.warning(f"CACHE FAILURE: Unable to reach Redis cluster: {str(re)}")

    # 3. Fallback to PostgreSQL
    logging.info(f"CACHE MISS: Fetching metrics for {service_name} from PostgreSQL storage.")
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT service_name, status, latency_ms, updated_at FROM system_metrics WHERE service_name = %s",
                (service_name,)
            )
            result = cur.fetchone()
        conn.close()
        
        if not result:
            logging.warning(f"NOT FOUND: Service {service_name} does not exist in relational schema.")
            raise HTTPException(status_code=404, detail="Service not found")
        
        # Serialize datetime object for JSON storage
        result['updated_at'] = str(result['updated_at'])
        
        # 4. Populate Redis Cache for next time
        try:
            redis_client.setex(service_name, 60, json.dumps(result)) # 60 second TTL
            logging.info(f"CACHE UPDATE: Cached metrics for {service_name} into Redis.")
        except redis.RedisError:
            pass
            
        return {"source": "database", "data": result}
        
    except psycopg2.Error as db_err:
        logging.error(f"DATABASE ERROR: Execution failed on query lookup: {str(db_err)}")
        raise HTTPException(status_code=500, detail="Database lookup failed")