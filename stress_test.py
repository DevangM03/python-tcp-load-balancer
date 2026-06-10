import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

LB_IP = "127.0.0.1"
LB_PORT = 8888
CONCURRENT_CONNECTIONS = 1000

def simulate_client_lifecycle(client_id: int):
    """Simulates a complete client lifecycle from load balancing to mutation."""
    try:
        # 1. Fetch server port assignment from Load Balancer
        lb_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lb_sock.connect((LB_IP, LB_PORT))
        lb_sock.send("CLIENT_REQUEST\n".encode())
        server_port = int(lb_sock.recv(1024).decode().strip())
        lb_sock.close()
        
        # 2. Open a session directly with the assigned backend node
        srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv_sock.connect((LB_IP, server_port))
        
        # 3. Perform a data write operation (PUT)
        srv_sock.send("PUT\n".encode())
        srv_sock.send(f"key_{client_id}\n".encode())
        srv_sock.send(f"val_{client_id}\n".encode())
        res_put = srv_sock.recv(1024).decode().strip()
        
        # 4. Perform a validation read operation (GET)
        srv_sock.send("GET\n".encode())
        srv_sock.send(f"key_{client_id}\n".encode())
        res_get = srv_sock.recv(1024).decode().strip()
        
        # 5. Terminate the connection cleanly
        srv_sock.send("QUIT\n".encode())
        srv_sock.close()
        
        return True
    except Exception:
        return False

def run_stress_test():
    print(f"Launching validation script: Injecting {CONCURRENT_CONNECTIONS} concurrent clients...")
    start_time = time.time()
    
    success_count = 0
    # Use ThreadPoolExecutor to run 1,000 tasks concurrently
    with ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(simulate_client_lifecycle, range(CONCURRENT_CONNECTIONS)))
        
    success_count = sum(1 for res in results if res)
    total_time = time.time() - start_time
    throughput = success_count / total_time
    
    print("\n================ SYSTEM TEST RESULTS ================")
    print(f"Total Attempted Connections  : {CONCURRENT_CONNECTIONS}")
    print(f"Successful Task Completions  : {success_count}")
    print(f"Failed Task Completions      : {CONCURRENT_CONNECTIONS - success_count}")
    print(f"Total Execution Runtime      : {total_time:.2f} seconds")
    print(f"Empirical System Throughput : {throughput:.2f} operations/sec")
    print("=====================================================")

if __name__ == "__main__":
    run_stress_test()