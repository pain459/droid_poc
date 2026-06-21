# Jarvis Target Stack & MCP Controller

This repository contains a Proof of Concept (PoC) target stack designed for evaluating AI-agent troubleshooting capabilities. It integrates a simulated microservice architecture with real-time telemetry, runtime fault injection controls, and a Model Context Protocol (MCP) server.

---

## Architecture Overview

```
                          +------------------------+
                          |   Browser Dashboard    |
                          | (HTML/WebSocket/SSE)   |
                          +-----------+------------+
                                      ^
                                      | WebSockets (Live Metrics & Logs)
                                      v
+--------------+ stdio    +-----------+------------+
|   AI Agent   |<-------->|   MCP Server (Host)    |
| (Claude/etc) |          +-----------+------------+
+--------------+                      |
                                      | HTTP REST / File Read
                                      v
                          +-----------+------------+
                          |  FastAPI Web Service   |
                          +-----+------------+-----+
                                |            |
                 PostgreSQL     v            v   Redis
                          +-----+----+   +---+-----+
                          | Database |   |  Cache  |
                          +----------+   +---------+
```

*   **FastAPI Application**: Serves endpoints, triggers background traffic simulation, and supports runtime fault injections.
*   **PostgreSQL**: Relational database storage representing persistent system metrics.
*   **Redis**: Key-value cache layer to optimize metrics retrieval.
*   **Operations Dashboard**: A premium, glassmorphic dark-mode interface with live WebSocket metrics and real-time backend log streaming.
*   **MCP Server**: A Model Context Protocol server exposing control tools to automated debugging agents.

---

## Getting Started

### Prerequisites
*   Docker & Docker Compose
*   Python 3.10+ (for host-level MCP server)

### Step 1: Configure Environment Variables

The AI Troubleshooting Chat Assistant requires an OpenAI API key to communicate with GPT-4o-mini.

1. Copy the example environment template to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and set your actual OpenAI API key:
   ```env
   OPENAI_API_KEY=sk-proj-your-key-here
   ```

---

### Step 2: Run the Docker Stack

Build and start the web-app, database, and cache containers:
```bash
docker compose up --build -d
```

Verify that the containers are healthy and running:
```bash
docker ps
```
The FastAPI web server is now exposed at `http://localhost:8000`.

---

### Step 3: Access the Operations Dashboard

Open your web browser and navigate to:
```
http://localhost:8000/
```
*   **Live Metrics**: Visualizes latencies, status badges (`HEALTHY`, `DEGRADED`, `CRITICAL`), request counts, and Redis cache hit ratios.
*   **Traffic Simulator**: A background task automatically runs simulated queries to keep the dashboard active.
*   **Live Console Log Feed**: Streams standard logs and errors from the container directly to the browser screen.

---

## Fault Injection Capabilities

Using either the **Dashboard Control Panel** or the **MCP Tools**, you can inject live faults into the target stack to test diagnostic pipelines:

| Fault Type | Parameter | Description |
| :--- | :--- | :--- |
| **Database Down** | `db_down` (boolean) | Simulates complete database connection/handshake failure. |
| **Redis Down** | `redis_down` (boolean) | Simulates Redis node disconnection. Disables caching, forcing DB lookups. |
| **Simulate CPU Spike** | `cpu_spike` (boolean) | Triggers execution spikes, locking application threads on lookup tasks. |
| **Simulate Memory Leak** | `memory_leak` (boolean) | Allocates ~15MB of random bytes per query, filling container memory. |
| **Database Latency** | `db_latency` (float) | Injects artificial delay (0.0 to 5.0 seconds) into database queries. |

Use the **"Clear All Active Faults"** button (or API call) to reset all injected faults and trigger Python garbage collection.

---

## Running the MCP Server

The Model Context Protocol (MCP) server allows AI agents (such as Claude Desktop or custom LLM frameworks) to connect to this target stack and interact with it.

### 1. Setup the Host Environment
Initialize a virtual environment and install the dependencies:
```bash
# Create a virtual environment
python3 -m venv venv

# Install MCP server requirements
venv/bin/pip install -r mcp_requirements.txt
```

### 2. Launch the MCP Server (stdio Transport)
By default, the server runs over standard input/output (stdio), which is the standard protocol transport used by LLM clients:
```bash
PATH=venv/bin:$PATH venv/bin/fastmcp run app/mcp_server.py
```

### 3. Debugging with the MCP Inspector
To run a local web-based client interface for testing the tools interactively:
```bash
PATH=venv/bin:$PATH venv/bin/fastmcp dev app/mcp_server.py
```
This will start the MCP inspector in your browser (usually at `http://localhost:5173`) allowing you to run and verify the tools.

---

## MCP Server Tools Reference

The MCP server registers the following tools:

1.  **`get_service_status`**:
    *   **Arguments**: `service_name: str` (e.g., `payment_gateway`, `auth_provider`, `notification_engine`)
    *   **Description**: Retrieves status, latency, and cache hit metrics.
2.  **`get_active_faults`**:
    *   **Arguments**: None
    *   **Description**: Returns a JSON object with the state of all injected faults.
3.  **`inject_fault`**:
    *   **Arguments**: `fault_name: str`, `value: Any`
    *   **Description**: Injects a target fault (e.g., `db_down=True`, `db_latency=3.5`).
4.  **`reset_all_faults`**:
    *   **Arguments**: None
    *   **Description**: Resets all faults back to default values.
5.  **`read_service_logs`**:
    *   **Arguments**: `lines_count: int` (default 50)
    *   **Description**: Reads the trailing lines of `service.log` directly from the host filesystem to diagnose backend behavior.
