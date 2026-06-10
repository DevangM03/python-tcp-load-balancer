# Dynamic TCP/IP Load Balancer & Distributed In-Memory Key-Value Store

A high-performance, lightweight, distributed in-memory caching engine and custom Layer-4 traffic router built completely from scratch using raw Python sockets, network programming interfaces, and native multi-threading. 

This project demonstrates low-level systems execution without relying on any external frameworks (like Django, Flask, or FastAPI), highlighting core fundamentals in transport layer protocols, concurrent data structures, and cluster coordination.

---

## 🏗️ System Architecture

The architecture utilizes a decoupled control and data plane split configured as a Layer-4 Redirect Router. Instead of proxying entire data payloads through the load balancer (which creates a massive network bottleneck), the load balancer acts as a swift connection broker, offloading direct client sessions directly onto target backend nodes.

```text
       +-------------------------+
       |   Automated Benchmark   |
       |      Driver Engine      |
       +------------+------------+
                    |
                    | 1. CLIENT_REQUEST (TCP Handshake)
                    v
       +-------------------------+
       |      Load Balancer      | <--- REGISTER:{PORT} --- [ Decoupled Storage Node ]
       |       (Port 8888)       |                          (Dynamic Discovery)
       +------------+------------+
                    |
                    | 2. Returns Target Allocation Port (e.g., 12345)
                    v
       +-------------------------+
       |   Automated Benchmark   |
       |      Driver Engine      |
       +------------+------------+
                    |
                    | 3. Binds Long-Lived Direct TCP Session
                    v
       +-------------------------+
       |   Active Storage Node   |
       |      (Port 12345)       | ---> [ Thread-Safe Generic HashMap Layer ]
       +-------------------------+
```

## 🧩 Core Components
* **load_balancer.py (The Router):** Orchestrates stateless traffic distribution across active cluster nodes using deterministic Round-Robin allocation rules. Includes a background active health checking engine.

* **server.py (The Storage Engine):** A high-throughput storage worker node handling independent client allocations inside dedicated OS-level threads. Features a custom generic HashMap data structure designed from first principles.

* **client.py (The Client Layer):** Features built-in structural state parsing and robust network connection retry recovery handlers.

* **stress_test.py (The Validation Driver):** An asynchronous concurrency test engine that saturates the socket layer to capture performance profiles.

## ⚡ Key Technical Highlights & Structural Design
**1. Thread-Safe HashMap with Dynamic Capacity Resizing**

Instead of using Python's heavily optimized built-in dictionary, this engine features a fully custom generic HashMap implementing Separate Chaining via nested list structures to handle hash collisions.

* **Reentrant Synchronization:** Operations (PUT, GET, REMOVE) are protected by a Reentrant Lock (threading.RLock). This guarantees absolute thread safety and prevents race conditions when a thread mutates a bucket list while another worker triggers an internal table structural expansion.

* **Dynamic O(1) Bound Control:** Tracks real-time density using a load factor calculation (size / capacity). Crossing a strict 0.75 threshold triggers an automated O(N) dynamic array expansion—doubling memory bounds and executing global bucket rehashing to guarantee average-case O(1) search complex profiles under dense parallel load.

**2. Dynamic Service Discovery Protocol**

The cluster features an empty routing table at startup. Backend storage instances utilize an autonomous initialization hook to establish direct control sockets back to the load balancer, broadcasting a custom REGISTER:{PORT} network payload. The load balancer dynamically provisions these identifiers into the round-robin routing loop, enabling stateless horizontal scaling.

**3. Fault Detection & Node Isolation**

The load balancer spawns an isolated background daemon thread executing active health probes. Every 3 seconds, it runs non-blocking TCP socket test connections against registered backend nodes. If a connection times out or throws an explicit network error, the health checker triggers an internal mutex lock, instantly evicts the broken target from the pool, and resets the pointer index boundaries to protect incoming client traffic from dropping.

## 📊 Performance Validation Under Load
The system's structural integrity and network capacity were validated under a high-concurrency automated thread-pool execution suite. The stress testing engine injected 1,000 parallel client lifecycles executing atomic mutation blocks against the data plane simultaneously.
```text
================ SYSTEM TEST RESULTS ================
Total Attempted Connections  : 1000
Successful Task Completions  : 1000
Failed Task Completions      : 0
Total Execution Runtime      : 0.98 seconds
Empirical System Throughput  : 1017.77 operations/sec
=====================================================
```
**Analysis:** Achieved a 100% success rate with zero failed operations or network dropped frames under concurrent stress. This empirically verifies both the connection capacity of the Layer-4 load balancer routing backlog and the structural locking integrity of the underlying generic memory layer during multi-threaded table resizing.

## 🚀 Quick Start & Verification Playbook
Verify this distributed environment locally by using multiple terminal panes to observe real-time routing and network handshakes:

**1. Start the Control Plane (Load Balancer)**
```bash
python load_balancer.py
```

**2. Spin Up Decoupled Storage Nodes**

Open separate terminal instances and launch your storage nodes on custom ports. Watch the load balancer recognize them instantly:
```bash
python server.py 12345
```
```bash
python server.py 12346
```

**3. Run the Benchmarking Utility**

Fire the high-concurrency simulation suite to evaluate throughput and thread safety limits:
```bash
python stress_test.py
```

## 🛠️ Tech Stack & Skills Highlighted
* **Core Language:** Python 3 (with modern static Type Hinting generics via the typing module)

* **Networking Layer:** Native OS Sockets (socket), TCP/IP Stream Multiplexing, Network Serialization

* **Concurrency Models:** Multi-threading Synchronization, Reentrant Mutual Exclusion (RLock), Daemon Workers, Thread Pool Scaling
