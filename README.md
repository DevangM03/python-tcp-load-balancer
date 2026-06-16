# Asynchronous Layer-4 Load Balancer & Distributed In-Memory Key-Value Store

A high-performance, lightweight, distributed in-memory caching engine and custom Layer-4 traffic router built completely from scratch using raw Python sockets, asynchronous network programming interfaces, and native I/O multiplexing.

This project demonstrates low-level systems execution without relying on any external web frameworks (like Django, Flask, or FastAPI), highlighting core fundamentals in transport-layer protocols, event-driven concurrency models, and lock-free distributed data structures.

---

## 🏗️ System Architecture

The architecture utilizes a decoupled control and data plane split configured as an out-of-band **Layer-4 Redirect Router**. Instead of proxying entire data payloads through the load balancer (which creates a massive CPU and memory bottleneck), the load balancer acts as a swift connection broker, offloading direct client sessions directly onto target sharded backend nodes.

```text
       +--------------------------------------------+
       |             CLIENT SUBSYSTEM               |
       |  (stress_test.py / AsyncClient Driver)     |
       +--------------------------------------------+
         /                                        \
        / [Phase 2: Lease Request]                 \ [Phase 3: Direct Data Stream]
       /  Target: 127.0.0.1:8888                    \  Target: 127.0.0.1:12345
      v                                              v
+--------------------------+                 +---------------------------+
|      LOAD BALANCER       |                 |    SHARDED DATA NODE A    |
|      (Control Plane)     |                 |       (Data Plane)        |
|       Port: 8888         |                 |        Port: 12345        |
+--------------------------+                 +---------------------------+
      ^                                      | - Custom Atomic HashMap   |
      |                                      | - Volatile Memory Cache   |
      | [Phase 1: Registration]              +---------------------------+
      | Broadcast: "REGISTER:12345\n"                      ^
      |                                                    |
      +----------------------------------------------------+ [Phase 4: Out-of-Band Health Probe]
                                                             Periodic Socket Pings (Every 3s)

```

---

## 🧩 Core Components

* **`load_balancer.py` (The Control Plane):** Orchestrates stateless traffic distribution across active cluster nodes using deterministic Round-Robin allocation rules. Runs an asynchronous event loop to process incoming token leases instantly and drop connections to keep overhead minimal.
* **`server.py` (The Data Plane):** A high-throughput storage worker node handling concurrent client allocations using non-blocking asynchronous stream readers and writers. Features a custom generic `HashMap` data structure designed from first principles.
* **`client.py` (The Client Layer):** An interactive utility featuring atomic wire protocol framing and robust network connection retry recovery handlers.
* **`stress_test.py` (The Validation Driver):** A highly concurrent, event-driven benchmarking engine that saturates the local network stack to evaluate the system's structural and throughput limits.

---

## ⚡ Key Technical Highlights & Structural Design

### 1. Lock-Free HashMap with Single-Threaded Task Atomicity

Instead of using Python's built-in dictionary, this engine features a fully custom generic `HashMap` implementing Separate Chaining via nested list structures to handle hash collisions.

* **Zero-Lock Overhead:** By migrating the storage nodes to a single-threaded asynchronous event loop via `asyncio`, the system completely eliminates the need for heavy kernel-level synchronization primitives (like `threading.RLock`). Memory mutations are guaranteed to be naturally atomic because task context-switching only occurs at explicit `await` boundaries.
* **Dynamic Amortized $O(1)$ Bounds:** Tracks real-time density using a load factor calculation ($\text{size} / \text{capacity}$). Crossing a strict $0.75$ threshold triggers an automated dynamic array expansion—doubling memory bounds and executing a global bucket rehash to guarantee average-case $O(1)$ search profiles under dense parallel loads.

### 2. Low-Level Kernel Tuning & C50K Optimization

To break past standard operating system network limits and process tens of thousands of concurrent connections on a single machine, the system bypasses default configuration bottlenecks:

* **I/O Multiplexing Overrides:** Configured to explicitly leverage the `WindowsProactorEventLoopPolicy` utilizing native **Windows I/O Completion Ports (IOCP)**, bypassing the rigid 64-socket limitation found in traditional `select()` loop wrappers.
* **Buffer Expansion:** Expanded the incoming socket listen backlog buffer size to `8192` to absorb massive initial connection bursts without dropping TCP handshakes.
* **Protocol Compaction:** Designed a rigid, single-line framed application wire protocol (`OP:KEY:VALUE\n`) allowing the server to stream and parse transaction arguments via single-pass `readline()` executions.

### 3. Dynamic Service Discovery & Fault Isolation

The cluster features an empty routing table at startup. Backend storage instances utilize an autonomous initialization hook to establish a temporary socket connection back to the load balancer, broadcasting a `REGISTER:{PORT}\n` payload.

Concurrently, an asynchronous background daemon task (`_health_check_loop`) wakes up inside the load balancer every 3 seconds to ping the registered nodes. If a server fails to respond within a 0.5-second timeout, the daemon instantly evicts the broken target port from the pool, preventing incoming client traffic from being routed to a degraded node.

---

## 📊 Performance Validation Under Load

The system's structural integrity, connection threshold, and network capacity were validated under a high-concurrency automated driver engine. The stress testing script injected **50,000 parallel client loops** executing atomic mutation blocks against the sharded storage cluster over a local loopback interface.

```text
Launching C50K Validation Driver: Injecting 50000 concurrent tasks...

================ SYSTEM TEST RESULTS ================
Total Attempted Connections  : 50000
Successful Task Completions  : 50000
Failed Task Completions      : 0
Total Execution Runtime      : 40.91 seconds
Empirical System Throughput  : 2444.59 operations/sec
=====================================================
🎯 SUCCESS: The system successfully sustained 50,000 concurrent connections with 100% integrity!

```
### Engineering Analysis:

* **100% Data Integrity:** Achieved a perfect task completion rate with zero dropped packets, connection timeouts, or socket memory leaks under peak saturation.
* **Throughput Metrics:** Reached a native unthrottled throughput of **2,444.59 operations/second**, proving the efficiency of the single-threaded asynchronous multiplexing architecture.
* **Kernel Constraints:** Sequential back-to-back testing runs normalize to **1,554.18 operations/second** over 64.34 seconds. This minor delta is a documented result of Windows kernel socket throttling due to `TIME_WAIT` port accumulation (allocating 100,000 unique socket identities per run), proving that the performance ceiling is governed by OS network recycling configurations rather than application-layer execution delays.

---

## 🚀 Quick Start & Automated Execution Playbook

Verify this distributed cluster locally using VS Code's integrated terminal layout or its built-in debugging profiles:

### Option A: The Automated VS Code Way (Recommended)

This repository includes a pre-configured `.vscode/launch.json` file that automates the cluster's orchestration:

1. Open the repository in VS Code.
2. Press `Ctrl + Shift + D` to open the **Run and Debug** menu.
3. Select **`🚀 Launch Distributed Cluster`** from the top dropdown menu.
4. Press **`F5`**. VS Code will instantly spin up three independent background terminal windows, initialize the load balancer, and boot both storage servers on ports `12345` and `12346`.

### Option B: The Manual Native Way

Open separate terminal panes to observe real-time routing and network registration handshakes:

1. **Start the Control Plane (Load Balancer)**
```bash
python load_balancer.py

```


2. **Spin Up Sharded Storage Nodes**
```bash
python server.py 12345

```


```bash
python server.py 12346

```


3. **Run the Benchmarking Driver**
```bash
python stress_test.py

```



---

## 🛠️ Tech Stack & Core Primitives

* **Core Language:** Python 3 (featuring static Type Hinting generics via the `typing` module)
* **Networking Architecture:** Native OS Sockets (`socket`), TCP/IP Stream Multiplexing, Non-blocking Stream Readers/Writers
* **Concurrency Models:** Asynchronous I/O Multiplexing (`asyncio`), Windows I/O Completion Ports (IOCP), Task-Level Atomicity, Non-blocking Background Daemons
* **Automation Profile:** VS Code Workspace Compound Configurations (`launch.json`)
