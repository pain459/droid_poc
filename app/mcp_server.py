import os
import httpx
from typing import Any
from fastmcp import FastMCP

# Initialize FastMCP Server
mcp = FastMCP("Jarvis Target Stack Controller")

# Use localhost if on host, otherwise use local container address if we want to be safe.
# But wait, inside the web-app container, localhost:8000 resolves to the FastAPI container itself! So localhost:8000 works perfectly in both places.
API_URL = "http://localhost:8000"

# Robust Log File Path Resolution
LOGS_PATH = "/var/log/app/service.log"
if not os.path.exists(LOGS_PATH):
    # Try host relative paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path_direct = os.path.join(base_dir, "logs", "service.log")
    path_parent = os.path.join(os.path.dirname(base_dir), "logs", "service.log")
    if os.path.exists(path_direct):
        LOGS_PATH = path_direct
    else:
        LOGS_PATH = path_parent

@mcp.tool()
async def get_service_status(service_name: str) -> dict:
    """
    Retrieve the current status, latency (ms), and data source for a given system service.
    
    Args:
        service_name (str): The name of the service to check (e.g., payment_gateway, auth_provider, notification_engine).
    """
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{API_URL}/service/{service_name}", timeout=10.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as hse:
            return {"error": f"Service returned error status code: {hse.response.status_code}", "detail": hse.response.text}
        except Exception as e:
            return {"error": f"Failed to reach target service: {str(e)}"}

@mcp.tool()
async def get_active_faults() -> dict:
    """
    Retrieve the status of all active faults currently injected into the system.
    """
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{API_URL}/api/faults", timeout=5.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": f"Failed to reach API: {str(e)}"}

@mcp.tool()
async def inject_fault(fault_name: str, value: Any) -> dict:
    """
    Inject or update a specific operational fault into the system.
    
    Args:
        fault_name (str): The identifier of the fault to toggle (db_down, redis_down, cpu_spike, memory_leak, db_latency).
        value (Any): The value for the fault. Use True/False for boolean triggers (db_down, redis_down, cpu_spike, memory_leak). Use a float representing seconds for db_latency (e.g., 2.5).
    """
    # Normalize strings passed as boolean
    if isinstance(value, str):
        if value.lower() == 'true':
            value = True
        elif value.lower() == 'false':
            value = False
        else:
            try:
                value = float(value)
            except ValueError:
                pass

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{API_URL}/api/faults",
                json={"fault": fault_name, "value": value},
                timeout=5.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as hse:
            return {"error": f"Failed to inject fault: {hse.response.status_code}", "detail": hse.response.text}
        except Exception as e:
            return {"error": f"Failed to reach API: {str(e)}"}

@mcp.tool()
async def reset_all_faults() -> dict:
    """
    Clear all active operational faults in the target system and run garbage collection.
    """
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{API_URL}/api/reset", timeout=5.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": f"Failed to reach API: {str(e)}"}

@mcp.tool()
def read_service_logs(lines_count: int = 50) -> str:
    """
    Read the last N lines of the shared system service log file (service.log) to check diagnostics, connection errors, or cache statuses.
    
    Args:
        lines_count (int): Number of trailing log lines to read. Default is 50.
    """
    if not os.path.exists(LOGS_PATH):
        return f"Log file not found at local workspace path: {LOGS_PATH}"
    
    try:
        with open(LOGS_PATH, "r") as f:
            lines = f.readlines()
            last_lines = lines[-lines_count:]
            return "".join(last_lines)
    except Exception as e:
        return f"Error reading log file: {str(e)}"

if __name__ == "__main__":
    mcp.run()
