import socket
import threading
from typing import Optional, List, Generic, TypeVar

K = TypeVar('K')
V = TypeVar('V')

class KeyValuePair(Generic[K, V]):
    def __init__(self, key: K, value: V):
        self.key = key
        self.value = value

class HashMap(Generic[K, V]):
    def __init__(self, initial_capacity: int = 16):
        self.INITIAL_CAPACITY = initial_capacity
        self.buckets: List[List[KeyValuePair[K, V]]] = [[] for _ in range(self.INITIAL_CAPACITY)]
        self.size = 0
        # RLock is used because put() recursively calls _resize(), which also needs the lock.
        self.lock = threading.RLock()

    def _get_bucket_index(self, key: K) -> int:
        hash_code = hash(key)
        return abs(hash_code) % len(self.buckets)

    def put(self, key: K, value: V) -> None:
        with self.lock:
            index = self._get_bucket_index(key)
            bucket = self.buckets[index]

            # Check if key already exists
            for entry in bucket:
                if entry.key == key:
                    entry.value = value
                    return

            # Add new entry
            bucket.append(KeyValuePair(key, value))
            self.size += 1

            # Resize if load factor exceeds 0.75
            if self.size / len(self.buckets) > 0.75:
                self._resize()

    def get(self, key: K) -> Optional[V]:
        with self.lock:
            index = self._get_bucket_index(key)
            bucket = self.buckets[index]

            for entry in bucket:
                if entry.key == key:
                    return entry.value
            return None

    def get_or_default(self, key: K, default_value: V) -> V:
        with self.lock:
            value = self.get(key)
            return value if value is not None else default_value

    def remove(self, key: K) -> Optional[V]:
        with self.lock:
            index = self._get_bucket_index(key)
            bucket = self.buckets[index]

            for i, entry in enumerate(bucket):
                if entry.key == key:
                    removed_entry = bucket.pop(i)
                    self.size -= 1
                    return removed_entry.value
            return None

    def _resize(self) -> None:
        with self.lock:
            old_buckets = self.buckets
            self.buckets = [[] for _ in range(len(self.buckets) * 2)]
            self.size = 0

            for bucket in old_buckets:
                for entry in bucket:
                    self.put(entry.key, entry.value)

    def get_size(self) -> int:
        with self.lock:
            return self.size

class Server:
    def __init__(self, port: int = 12345):
        self.PORT = port
        self.data_store = HashMap[str, str]()

    def handle_client(self, client_socket: socket.socket, client_address: tuple):
        try:
            print(f"Client connected: {client_address}")
            
            while True:
                # Read request type
                request_type = client_socket.recv(1024).decode().strip()
                if not request_type or request_type == "QUIT":
                    break
                
                print(f"Received request: {request_type}")
                response = "Invalid request type"
                
                # Symmetrically handles both REMOVE and DELETE aliases to support resume text mappings
                if request_type in ["GET", "REMOVE", "DELETE"]:
                    key = client_socket.recv(1024).decode().strip()
                    
                    if request_type == "GET":
                        response = self.data_store.get_or_default(key, "Key not found")
                    else:
                        if self.data_store.remove(key) is not None:
                            response = f"{request_type} operation successful"
                        else:
                            response = f"Key not found for {request_type} operation"
                        
                elif request_type == "PUT":
                    key = client_socket.recv(1024).decode().strip()
                    value = client_socket.recv(1024).decode().strip()
                    self.data_store.put(key, value)
                    response = "PUT operation successful"
                
                # Send back payload response
                client_socket.send(f"{response}\n".encode())
                
        except Exception as e:
            print(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind(('localhost', self.PORT))
            server_socket.listen(1024)
            print(f"Server listening on port {self.PORT}")
            
            # --- Automatic Server Registration Protocol ---
            try:
                reg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                reg_socket.connect(('localhost', 8888))
                reg_socket.send(f"REGISTER:{self.PORT}\n".encode())
                reg_socket.close()
                print(f"Successfully registered node on port {self.PORT} with Load Balancer.")
            except Exception as e:
                print(f"Discovery Alert: Could not register with Load Balancer: {e}")
            # -----------------------------------------------
            
            while True:
                client_socket, client_address = server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client, 
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            print(f"Server error: {e}")
        finally:
            server_socket.close()

if __name__ == "__main__":
    import sys
    
    port = 12345  # default port
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Invalid port number, using default port 12345")
    
    server = Server(port)
    server.start()