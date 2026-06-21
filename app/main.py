import os
import time
import logging
import json
import random
import asyncio
from typing import List, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
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

# Active connections for WebSockets
active_metric_connections: List[WebSocket] = []
active_log_connections: List[WebSocket] = []

async def broadcast_log(message: str):
    disconnected = []
    for ws in list(active_log_connections):
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in active_log_connections:
            active_log_connections.remove(ws)

# Custom log handler to push logs over WebSocket
class WebSocketLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
            loop = asyncio.get_running_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(broadcast_log(log_entry), loop)
        except Exception:
            pass

ws_handler = WebSocketLogHandler()
ws_handler.setLevel(logging.INFO)
ws_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger().addHandler(ws_handler)

# Global metrics state
METRICS_STATE = {
    "payment_gateway": {
        "status": "HEALTHY",
        "latency_ms": 45,
        "source": "database",
        "timestamp": time.time(),
        "request_count": 0,
        "cache_hits": 0
    },
    "auth_provider": {
        "status": "DEGRADED",
        "latency_ms": 1200,
        "source": "database",
        "timestamp": time.time(),
        "request_count": 0,
        "cache_hits": 0
    },
    "notification_engine": {
        "status": "HEALTHY",
        "latency_ms": 12,
        "source": "database",
        "timestamp": time.time(),
        "request_count": 0,
        "cache_hits": 0
    }
}

async def broadcast_metrics():
    disconnected = []
    message = json.dumps(METRICS_STATE)
    for ws in list(active_metric_connections):
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in active_metric_connections:
            active_metric_connections.remove(ws)

# Fault Injection State
LEAKY_LIST = []
ACTIVE_FAULTS = {
    "db_latency": 0.0,
    "redis_down": False,
    "db_down": False,
    "cpu_spike": False,
    "memory_leak": False
}

# Lifespan context manager for startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start traffic simulator
    sim_task = asyncio.create_task(traffic_simulator())
    logging.info("FastAPI service started with background traffic simulator.")
    yield
    # Clean up
    sim_task.cancel()
    try:
        await sim_task
    except asyncio.CancelledError:
        pass
    logging.info("FastAPI service shutdown complete.")

app = FastAPI(title="Jarvis Target Stack", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# Initialize clients
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_db_connection():
    if ACTIVE_FAULTS["db_down"]:
        raise psycopg2.OperationalError("Simulated connection timeout/refused")
    return psycopg2.connect(
        host="postgres",
        database="inventory_db",
        user="admin",
        password="secretpassword"
    )

async def perform_service_lookup(service_name: str, force_error: bool = False):
    start_time = time.time()
    
    # 1. Database Handshake Failure / DB Down fault
    if force_error or ACTIVE_FAULTS["db_down"]:
        logging.error(f"CRITICAL: Failed connection handshake to database storage layer for service: {service_name}")
        METRICS_STATE[service_name].update({
            "status": "CRITICAL",
            "latency_ms": int((time.time() - start_time) * 1000),
            "source": "error"
        })
        await broadcast_metrics()
        raise HTTPException(status_code=500, detail="Internal Database Error Connection Timeout")

    # 2. CPU Spike Fault
    if ACTIVE_FAULTS["cpu_spike"]:
        logging.warning("FAULT ACTIVE: Simulating CPU Spike (heavy calculation)...")
        # Spin CPU for ~200ms
        t_end = time.time() + 0.2
        while time.time() < t_end:
            _ = [x * x for x in range(1000)]
            await asyncio.sleep(0.001)

    # 3. Memory Leak Fault
    if ACTIVE_FAULTS["memory_leak"]:
        logging.warning("FAULT ACTIVE: Simulating memory leak (allocating 15MB)...")
        # Allocate 15MB of random bytes
        LEAKY_LIST.append(os.urandom(15 * 1024 * 1024))

    # 4. Redis Cache Layer Check
    if not ACTIVE_FAULTS["redis_down"]:
        try:
            cached_data = redis_client.get(service_name)
            if cached_data:
                logging.info(f"CACHE HIT: Retrieved metrics for {service_name} from Redis.")
                elapsed = int((time.time() - start_time) * 1000)
                METRICS_STATE[service_name].update({
                    "status": "HEALTHY" if elapsed < 500 else "DEGRADED",
                    "latency_ms": elapsed,
                    "source": "cache"
                })
                METRICS_STATE[service_name]["request_count"] += 1
                METRICS_STATE[service_name]["cache_hits"] += 1
                await broadcast_metrics()
                return {"source": "cache", "data": json.loads(cached_data)}
        except redis.RedisError as re:
            logging.warning(f"CACHE FAILURE: Unable to reach Redis cluster: {str(re)}")
    else:
        logging.warning("FAULT ACTIVE: Redis cache connection refused.")

    # 5. Database Latency Fault
    if ACTIVE_FAULTS["db_latency"] > 0:
        latency_to_inject = ACTIVE_FAULTS["db_latency"]
        logging.warning(f"FAULT ACTIVE: Injecting DB latency of {latency_to_inject}s...")
        await asyncio.sleep(latency_to_inject)

    # 6. Fallback to PostgreSQL
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
            METRICS_STATE[service_name].update({
                "status": "CRITICAL",
                "latency_ms": int((time.time() - start_time) * 1000),
                "source": "error"
            })
            await broadcast_metrics()
            raise HTTPException(status_code=404, detail="Service not found")
        
        # Serialize datetime object
        result['updated_at'] = str(result['updated_at'])
        
        # 7. Populate Redis Cache for next time
        if not ACTIVE_FAULTS["redis_down"]:
            try:
                redis_client.setex(service_name, 60, json.dumps(result))
                logging.info(f"CACHE UPDATE: Cached metrics for {service_name} into Redis.")
            except redis.RedisError:
                pass
        
        elapsed = int((time.time() - start_time) * 1000)
        status = result['status']
        if elapsed > 1000:
            status = "DEGRADED"
        if elapsed > 3000:
            status = "CRITICAL"

        METRICS_STATE[service_name].update({
            "status": status,
            "latency_ms": elapsed,
            "source": "database"
        })
        METRICS_STATE[service_name]["request_count"] += 1
        await broadcast_metrics()
            
        return {"source": "database", "data": result}
        
    except (psycopg2.Error, psycopg2.OperationalError) as db_err:
        logging.error(f"DATABASE ERROR: Execution failed on query lookup: {str(db_err)}")
        METRICS_STATE[service_name].update({
            "status": "CRITICAL",
            "latency_ms": int((time.time() - start_time) * 1000),
            "source": "error"
        })
        await broadcast_metrics()
        raise HTTPException(status_code=500, detail="Database lookup failed")

async def traffic_simulator():
    logging.info("Traffic simulator background task started.")
    services = ['payment_gateway', 'auth_provider', 'notification_engine']
    while True:
        try:
            service = random.choice(services)
            await perform_service_lookup(service)
        except Exception:
            pass
        await asyncio.sleep(random.uniform(1.0, 2.5))

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/service/{service_name}")
async def get_service_status(service_name: str, force_error: bool = False):
    logging.info(f"Received API request for service: {service_name}")
    return await perform_service_lookup(service_name, force_error)

class FaultPayload(BaseModel):
    fault: str
    value: Any

@app.post("/api/faults")
async def set_fault(payload: FaultPayload):
    fault_name = payload.fault
    if fault_name not in ACTIVE_FAULTS:
        raise HTTPException(status_code=400, detail="Invalid fault type")
    
    val = payload.value
    ACTIVE_FAULTS[fault_name] = val
    logging.warning(f"FAULT CONFIG CHANGE: Set {fault_name} = {val}")
    return {"status": "success", "faults": ACTIVE_FAULTS}

@app.get("/api/faults")
async def get_faults():
    return ACTIVE_FAULTS

@app.post("/api/reset")
async def reset_faults():
    global LEAKY_LIST
    for k in ACTIVE_FAULTS:
        if isinstance(ACTIVE_FAULTS[k], bool):
            ACTIVE_FAULTS[k] = False
        elif isinstance(ACTIVE_FAULTS[k], (int, float)):
            ACTIVE_FAULTS[k] = 0.0
    
    LEAKY_LIST.clear()
    import gc
    gc.collect()
    logging.warning("FAULT RESET: All system faults cleared and memory leak collection triggered.")
    return {"status": "success", "faults": ACTIVE_FAULTS}

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    active_metric_connections.append(websocket)
    try:
        # Send initial metrics state immediately
        await websocket.send_text(json.dumps(METRICS_STATE))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_metric_connections:
            active_metric_connections.remove(websocket)

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await websocket.accept()
    active_log_connections.append(websocket)
    try:
        # Read the last few logs from the file for startup context
        log_path = "/var/log/app/service.log"
        if os.path.exists(log_path):
            try:
                with open(log_path, "r") as f:
                    lines = f.readlines()[-30:]
                    for line in lines:
                        await websocket.send_text(line.strip())
            except Exception:
                pass
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_log_connections:
            active_log_connections.remove(websocket)