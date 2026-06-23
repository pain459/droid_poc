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

# OpenAI and MCP Client imports
from openai import AsyncOpenAI
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

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

# Initialize OpenAI client
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY") or "dummy-key-missing")

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
    return templates.TemplateResponse(request=request, name="index.html")

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

@app.get("/agent", response_class=HTMLResponse)
async def get_agent_workspace(request: Request):
    return templates.TemplateResponse(request=request, name="agent.html")

# OpenAI Chat Endpoint Models
class ChatInput(BaseModel):
    message: str
    session_id: str = "default"

class SessionDeleteInput(BaseModel):
    session_id: str

class SessionRenameInput(BaseModel):
    session_id: str
    title: str

import uuid
from datetime import datetime

# Global agent chat sessions storage
default_sid = "default"
AGENT_SESSIONS = {
    default_sid: {
        "id": default_sid,
        "title": "Default Session",
        "created_at": datetime.utcnow().isoformat(),
        "messages": []
    }
}

@app.get("/api/chat/sessions")
async def get_chat_sessions():
    return [
        {
            "id": s["id"],
            "title": s["title"],
            "created_at": s["created_at"],
            "message_count": len(s["messages"])
        }
        for s in AGENT_SESSIONS.values()
    ]

@app.post("/api/chat/sessions/create")
async def create_chat_session():
    sid = str(uuid.uuid4())
    title = f"Diagnostic Session {len(AGENT_SESSIONS) + 1}"
    AGENT_SESSIONS[sid] = {
        "id": sid,
        "title": title,
        "created_at": datetime.utcnow().isoformat(),
        "messages": []
    }
    return AGENT_SESSIONS[sid]

@app.post("/api/chat/sessions/delete")
async def delete_chat_session(payload: SessionDeleteInput):
    sid = payload.session_id
    if sid == "default":
        AGENT_SESSIONS["default"]["messages"] = []
        AGENT_SESSIONS["default"]["title"] = "Default Session"
    elif sid in AGENT_SESSIONS:
        del AGENT_SESSIONS[sid]
    return {"status": "success"}

@app.post("/api/chat/sessions/rename")
async def rename_chat_session(payload: SessionRenameInput):
    sid = payload.session_id
    if sid in AGENT_SESSIONS:
        AGENT_SESSIONS[sid]["title"] = payload.title
        return {"status": "success", "title": payload.title}
    raise HTTPException(status_code=404, detail="Session not found")

@app.get("/api/chat/sessions/{session_id}/history")
async def get_session_history(session_id: str):
    if session_id not in AGENT_SESSIONS:
        raise HTTPException(status_code=404, detail="Session not found")
    return AGENT_SESSIONS[session_id]["messages"]

@app.post("/api/chat")
async def chat_with_agent(payload: ChatInput):
    if not os.getenv("OPENAI_API_KEY"):
        return {
            "role": "assistant",
            "content": "⚠️ **OpenAI API Key is missing!** Please set the `OPENAI_API_KEY` environment variable in your host environment and rebuild the containers using `docker compose up --build -d`.",
            "tool_calls": []
        }

    sid = payload.session_id
    if sid not in AGENT_SESSIONS:
        sid = "default"

    # Enforce context capacity limit (e.g. 15,000 characters)
    total_length = sum(len(m.get("content", "")) for m in AGENT_SESSIONS[sid]["messages"])
    if total_length >= 15000:
        return {
            "role": "assistant",
            "content": "⚠️ **Context capacity limit reached for this session (15,000 characters).** To prevent excessive API token costs, please clear this session or start a new diagnostic session in the left sidebar.",
            "tool_calls": []
        }

    # If first user message, dynamically rename the session title
    if not AGENT_SESSIONS[sid]["messages"]:
        preview = payload.message.strip()
        if len(preview) > 30:
            preview = preview[:27] + "..."
        AGENT_SESSIONS[sid]["title"] = preview

    # Append user message to global history
    AGENT_SESSIONS[sid]["messages"].append({
        "role": "user",
        "content": payload.message
    })

    system_prompt = {
        "role": "system",
        "content": (
            "You are an expert AI site reliability engineer and troubleshooting assistant for the Jarvis Target Stack.\n"
            "You have access to tools that check the health of system services (payment_gateway, auth_provider, notification_engine), "
            "read trailing service log files, and inspect active system faults.\n"
            "Your main objective is DIAGNOSIS and EXPLANATION of faults to help the operator understand what went wrong, "
            "NOT automatically repairing the system.\n"
            "When a user asks you to investigate, troubleshoot, or check health:\n"
            "1. Use tools like `get_service_status`, `get_active_faults`, and `read_service_logs` to analyze the system state.\n"
            "2. Explain your reasoning and diagnostics in detail. Identify exactly which services are failing or degraded, pointing out relevant latencies, sources, and log errors.\n"
            "3. Propose a root-cause explanation (e.g. database down, Redis cache down, CPU throttling) and recommend corrective actions.\n"
            "4. DO NOT call `reset_all_faults` or modify the system state unless the user explicitly commands you to perform a repair action."
        )
    }
    
    # Reconstruct messages history for OpenAI
    messages = [system_prompt]
    for msg in AGENT_SESSIONS[sid]["messages"]:
        messages.append({
            "role": msg["role"],
            "content": msg["content"]
        })

    server_params = StdioServerParameters(
        command="python",
        args=["/app/mcp_server.py"]
    )

    tool_calls_log = []

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Fetch MCP Tools
                tools_result = await session.list_tools()
                openai_tools = []
                for tool in tools_result.tools:
                    openai_tools.append({
                        "type": "function",
                        "function": {
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.inputSchema
                        }
                    })
                    
                # Run conversation loop (max 5 turns to prevent infinite recursion)
                max_turns = 5
                for _ in range(max_turns):
                    response = await openai_client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=messages,
                        tools=openai_tools if openai_tools else None,
                        tool_choice="auto" if openai_tools else None,
                        temperature=0.2
                    )
                    
                    response_message = response.choices[0].message
                    
                    # Store assistant message
                    messages.append(response_message)
                    
                    if response_message.tool_calls:
                        # Process Tool calls via the MCP Stdio Session
                        for tool_call in response_message.tool_calls:
                            tool_name = tool_call.function.name
                            tool_args = json.loads(tool_call.function.arguments)
                            
                            logging.warning(f"AI AGENT MCP TOOL CALL: {tool_name}({tool_args})")
                            
                            try:
                                tool_result = await session.call_tool(tool_name, tool_args)
                                result_text = ""
                                for item in tool_result.content:
                                    if hasattr(item, "text"):
                                        result_text += item.text
                                    elif isinstance(item, dict) and "text" in item:
                                        result_text += item["text"]
                                        
                                # Log tool call and result
                                tool_calls_log.append({
                                    "name": tool_name,
                                    "arguments": tool_args,
                                    "result": result_text
                                })

                                messages.append({
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "name": tool_name,
                                    "content": result_text
                                })
                            except Exception as te:
                                logging.error(f"Error calling MCP tool {tool_name}: {str(te)}")
                                tool_calls_log.append({
                                    "name": tool_name,
                                    "arguments": tool_args,
                                    "result": f"Error: {str(te)}"
                                })
                                messages.append({
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "name": tool_name,
                                    "content": json.dumps({"error": f"Failed to execute tool: {str(te)}"})
                                })
                        continue
                    else:
                        # Final text answer
                        assistant_response = {
                            "role": "assistant",
                            "content": response_message.content,
                            "tool_calls": tool_calls_log
                        }
                        AGENT_SESSIONS[sid]["messages"].append(assistant_response)
                        return assistant_response
                        
                assistant_response = {
                    "role": "assistant",
                    "content": response_message.content or "Error: Conversation exceeded tool execution limit.",
                    "tool_calls": tool_calls_log
                }
                AGENT_SESSIONS[sid]["messages"].append(assistant_response)
                return assistant_response
                
    except Exception as e:
        logging.error(f"Agent conversation loop error: {str(e)}")
        return {
            "role": "assistant",
            "content": f"⚠️ **Error running AI agent session:** {str(e)}",
            "tool_calls": tool_calls_log
        }

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