# Dynamic TCP/IP Load Balancer & Distributed KV Store

A custom, multi-threaded dynamic load balancing system implemented in pure Python. This project manages client-server communication using a Round-Robin routing algorithm over TCP/IP sockets and features a fully distributed key-value store. 

Designed for scalable concurrent client handling, robust server discovery, and thread-safe data operations.

## 🚀 Key Features

* **Dynamic Load Balancing:** Implements a Round-Robin scheduling algorithm to evenly distribute incoming client TCP traffic across multiple active backend servers.
* **Multi-Threaded Architecture:** Utilizes Python's `threading` module to handle concurrent client connections in a highly scalable and thread-safe manner without blocking the main event loop.
* **Automatic Server Discovery:** Features a modular client-server protocol where backend server nodes dynamically register with the load balancer upon startup, ensuring seamless connection management and fault tolerance.
* **Distributed Key-Value Store:** Includes a custom, thread-safe HashMap implementation supporting concurrent `GET`, `PUT`, and `REMOVE` operations across the distributed nodes.

## 🛠️ Technology Stack

* **Language:** Python 3.x
* **Networking:** TCP/IP, `socket` programming
* **Concurrency:** `threading`, Thread-safe locks/mutexes
* **Architecture:** Distributed Systems, Client-Server Model

## 🧠 System Architecture

The system operates across three primary components:

1. **The Load Balancer (`load_balancer.py`):** Acts as the central gateway. It listens for incoming client connections and dynamically routes them to the next available server node using the Round-Robin algorithm. It also maintains a registry of active servers.
2. **The Server Nodes (`server.py`):** The backend workers. Each server connects to the load balancer to announce its availability and hosts a partition of the distributed key-value store, safely processing concurrent data requests.
3. **The Client (`client.py`):** Connects directly to the load balancer to issue `GET`, `PUT`, or `REMOVE` commands without needing to know the specific IP or port of the underlying backend servers.

## ⚙️ How to Run Locally

**1. Clone the repository:**
```bash
git clone [https://github.com/DevangM03/python-tcp-load-balancer.git](https://github.com/DevangM03/python-tcp-load-balancer.git)
cd python-tcp-load-balancer
```

**2. Start the Server Nodes:**
Open multiple terminal instances to simulate a distributed environment. Run a server instance in each:
```bash
python server.py
```
*(Note: Ensure you configure different ports if running on the same machine, or rely on the script's default dynamic port allocation if implemented).*

**3. Start the Load Balancer:**
In a new terminal, initialize the load balancer so it can start accepting server registrations and client traffic:
```bash
python load_balancer.py
```

**4. Run the Client:**
Finally, spin up one or more clients to send traffic through the load balancer:
```bash
python client.py
```
