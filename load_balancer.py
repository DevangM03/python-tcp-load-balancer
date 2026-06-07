import socket
import threading
from typing import List

class LoadBalancer:
    def __init__(self, lb_port: int = 8888):
        self.LB_PORT = lb_port
        # Two servers for round-robin load balancing
        self.server_addresses: List[int] = [12345, 12346]
        self.current_index = 0

    def handle_client(self, client_socket: socket.socket, client_address: tuple):
        try:
            print("Client connected, assigning server...")
            
            # Get current server port using round-robin
            server_port = self.server_addresses[self.current_index]
            print(f"Client was assigned Server: {server_port}")
            
            # Update index for next client
            self.current_index = (self.current_index + 1) % len(self.server_addresses)
            
            # Send server port to client
            client_socket.send(f"{server_port}\n".encode())
            
        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()

    def start(self):
        lb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lb_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            lb_socket.bind(('localhost', self.LB_PORT))
            lb_socket.listen(5)
            print(f"Load Balancer listening on port {self.LB_PORT}")
            
            while True:
                print("Waiting for client to connect...")
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