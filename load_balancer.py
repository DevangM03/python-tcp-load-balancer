import socket
import threading
import time
from typing import List

class LoadBalancer:
    def __init__(self, lb_port: int = 8888):
        self.LB_PORT = lb_port
        # Explicitly empty; populated dynamically via programmatic runtime registrations
        self.server_addresses: List[int] = []
        self.current_index = 0
        self.lock = threading.Lock() # Thread protection lock for dynamic pool updates

    def handle_client(self, client_socket: socket.socket, client_address: tuple):
        try:
            payload = client_socket.recv(1024).decode().strip()
            if not payload:
                return

            # Dynamic Discovery Handler Block
            if payload.startswith("REGISTER:"):
                try:
                    server_port = int(payload.split(":")[1])
                    with self.lock:
                        if server_port not in self.server_addresses:
                            self.server_addresses.append(server_port)
                    print(f"Dynamic Discovery: Registered storage node on port {server_port}")
                except ValueError:
                    print("Malformed registration payload format discarded.")
                return

            # Client Routing Handler Block
            if payload == "CLIENT_REQUEST":
                with self.lock:
                    if not self.server_addresses:
                        client_socket.send("ERROR: No backend nodes currently available\n".encode())
                        return
                    
                    # Round-Robin algorithm selection pointer mapping
                    server_port = self.server_addresses[self.current_index]
                    self.current_index = (self.current_index + 1) % len(self.server_addresses)
                
                print(f"Routing transaction {client_address} -> Target Port {server_port}")
                client_socket.send(f"{server_port}\n".encode())
                
        except Exception as e:
            print(f"Error executing load balancer scheduling logic: {e}")
        finally:
            client_socket.close()

    def _health_check_loop(self):
        """Runs continuously in a daemon thread to actively clear out broken servers."""
        while True:
            time.sleep(3)
            with self.lock:
                current_pool = list(self.server_addresses)
            
            for port in current_pool:
                try:
                    # Execute active network heartbeats via swift connection test
                    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    probe.settimeout(0.5)
                    probe.connect(('localhost', port))
                    probe.close()
                except (socket.timeout, ConnectionRefusedError):
                    print(f"Fault Detection: Storage node on port {port} dropped connection. Removing from pool.")
                    with self.lock:
                        if port in self.server_addresses:
                            self.server_addresses.remove(port)
                            # Boundary reset protection logic for the array tracking index
                            if self.current_index >= len(self.server_addresses) and len(self.server_addresses) > 0:
                                self.current_index = 0

    def start(self):
        # Spawns background health monitoring thread
        health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        health_thread.start()

        lb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lb_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            lb_socket.bind(('localhost', self.LB_PORT))
            lb_socket.listen(1024) # Expanded connection queue backlog size for stress handling
            print(f"Load Balancer listening on port {self.LB_PORT}")
            
            while True:
                client_socket, client_address = lb_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            print(f"Load Balancer error: {e}")
        finally:
            lb_socket.close()

if __name__ == "__main__":
    load_balancer = LoadBalancer()
    load_balancer.start()