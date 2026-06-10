import socket
import time

class Client:
    def __init__(self, server_ip: str = "127.0.0.1", lb_port: int = 8888):
        self.SERVER_IP = server_ip
        self.LB_PORT = lb_port

    def fetch_server_port(self) -> int:
        """Fetches an active storage destination node assignment from the load balancer."""
        lb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lb_socket.connect((self.SERVER_IP, self.LB_PORT))
        
        # Identity payload handshake signaling client routing traffic
        lb_socket.send("CLIENT_REQUEST\n".encode())
        
        response = lb_socket.recv(1024).decode().strip()
        lb_socket.close()
        
        if response.startswith("ERROR"):
            raise RuntimeError(response)
        return int(response)

    def start(self):
        server_socket = None
        max_retries = 3
        
        # Robust network retry fallback execution loop
        for attempt in range(max_retries):
            try:
                print(f"Routing request to load balancer (Attempt {attempt + 1}/{max_retries})...")
                server_port = self.fetch_server_port()
                
                print(f"Establishing direct connection session with node port: {server_port}")
                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.connect((self.SERVER_IP, server_port))
                print(f"Session safely bonded with Server Node: {server_port}")
                break
                
            except (ConnectionRefusedError, socket.timeout, RuntimeError) as e:
                print(f"Graceful Node Recovery: Failed to reach assigned server node ({e}). Fetching alternative target...")
                time.sleep(1)
        else:
            print("System Failure Alert: Failed to connect to any backend storage services.")
            return

        try:
            while True:
                # Symmetrically maps interaction inputs to the execution requirements of the backend store
                print("\nChoose operation (GET, PUT, REMOVE, DELETE) or 'QUIT' to exit:")
                operation = input().upper()
                
                if operation == "QUIT":
                    server_socket.send("QUIT\n".encode())
                    break
                
                if operation not in ["GET", "PUT", "REMOVE", "DELETE"]:
                    print("Invalid operation selection.")
                    continue
                
                server_socket.send(f"{operation}\n".encode())
                
                if operation in ["GET", "REMOVE", "DELETE"]:
                    print("Enter key:")
                    key = input()
                    server_socket.send(f"{key}\n".encode())
                    
                elif operation == "PUT":
                    print("Enter key:")
                    key = input()
                    print("Enter value:")
                    value = input()
                    server_socket.send(f"{key}\n".encode())
                    server_socket.send(f"{value}\n".encode())
                
                response = server_socket.recv(1024).decode().strip()
                print(f"Server response: {response}")
                
            server_socket.close()
            print("Client session disconnected cleanly.")
            
        except Exception as e:
            print(f"Fatal exception during runtime data streaming: {e}")

if __name__ == "__main__":
    client = Client()
    client.start()