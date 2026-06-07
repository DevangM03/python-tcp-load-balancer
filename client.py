import socket

class Client:
    def __init__(self, server_ip: str = "127.0.0.1", lb_port: int = 8888):
        self.SERVER_IP = server_ip
        self.LB_PORT = lb_port

    def start(self):
        try:
            # Connect to load balancer first
            lb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lb_socket.connect((self.SERVER_IP, self.LB_PORT))
            
            print("Connected to load balancer.")
            print("Waiting to connect to Server...")
            
            # Get server port from load balancer
            server_port = int(lb_socket.recv(1024).decode().strip())
            print(f"Connected to Server: {server_port}")
            
            lb_socket.close()
            
            # Connect to assigned server
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((self.SERVER_IP, server_port))
            
            while True:
                print("\nChoose operation (GET, PUT, REMOVE) or 'QUIT' to exit:")
                operation = input().upper()
                
                if operation == "QUIT":
                    server_socket.send("QUIT\n".encode())
                    break
                
                server_socket.send(f"{operation}\n".encode())
                
                if operation in ["GET", "REMOVE"]:
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
                    
                else:
                    print("Invalid operation")
                    continue
                
                # Receive response
                response = server_socket.recv(1024).decode().strip()
                print(f"Server response: {response}")
            
            server_socket.close()
            print("Disconnected from server.")
            
        except Exception as e:
            print(f"Client error: {e}")

if __name__ == "__main__":
    client = Client()
    client.start()

