import asyncio
import logging
import sys
from typing import Optional, List, Generic, TypeVar

# Configure high-performance logging infrastructure
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Data-Engine: %(message)s"
)

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
        
        # NOTE: threading.RLock is completely removed. 
        # In a single-threaded async event loop, synchronous operations are naturally atomic.

    def _get_bucket_index(self, key: K) -> int:
        return abs(hash(key)) % len(self.buckets)

    def put(self, key: K, value: V) -> None:
        index = self._get_bucket_index(key)
        bucket = self.buckets[index]

        # Key overwrite mutation scan
        for entry in bucket:
            if entry.key == key:
                entry.value = value
                return

        # Handle hash collisions smoothly using separate chaining
        bucket.append(KeyValuePair(key, value))
        self.size += 1

        # Evaluate load factor density; dynamically rehash if bounds cross the 0.75 threshold
        if self.size / len(self.buckets) > 0.75:
            self._resize()

    def get(self, key: K) -> Optional[V]:
        index = self._get_bucket_index(key)
        bucket = self.buckets[index]

        for entry in bucket:
            if entry.key == key:
                return entry.value
        return None

    def get_or_default(self, key: K, default_value: V) -> V:
        value = self.get(key)
        return value if value is not None else default_value

    def remove(self, key: K) -> Optional[V]:
        index = self._get_bucket_index(key)
        bucket = self.buckets[index]

        for i, entry in enumerate(bucket):
            if entry.key == key:
                removed_entry = bucket.pop(i)
                self.size -= 1
                return removed_entry.value
        return None

    def _resize(self) -> None:
        """Executes global bucket rehashing. Time Complexity: O(N) amortized."""
        old_buckets = self.buckets
        self.buckets = [[] for _ in range(len(self.buckets) * 2)]
        self.size = 0

        for bucket in old_buckets:
            for entry in bucket:
                self.put(entry.key, entry.value)

    def get_size(self) -> int:
        return self.size


class AsyncServer:
    def __init__(self, port: int = 12345, lb_host: str = '127.0.0.1', lb_port: int = 8888):
        self.PORT = port
        self.LB_HOST = lb_host
        self.LB_PORT = lb_port
        self.data_store = HashMap[str, str]()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Processes atomic, line-terminated data commands inside non-blocking network streams."""
        client_address = writer.get_extra_info('peername')
        try:
            while True:
                # Read incoming data chunks up to the newline character delimiter (\n)
                data = await reader.readline()
                if not data:
                    break
                
                payload = data.decode().strip()
                if payload == "QUIT" or not payload:
                    break
                
                # Parse single-line message framing (e.g., PUT:key:value or GET:key)
                parts = payload.split(":", 2)
                request_type = parts[0].upper()
                response = "Invalid request format"

                if request_type in ["GET", "REMOVE", "DELETE"] and len(parts) >= 2:
                    key = parts[1]
                    if request_type == "GET":
                        response = self.data_store.get_or_default(key, "Key not found")
                    else:
                        if self.data_store.remove(key) is not None:
                            response = f"{request_type} operation successful"
                        else:
                            response = f"Key not found for {request_type} operation"

                elif request_type == "PUT" and len(parts) == 3:
                    key, value = parts[1], parts[2]
                    self.data_store.put(key, value)
                    response = "PUT operation successful"

                # Write line-terminated payload frame back down the socket pipeline
                writer.write(f"{response}\n".encode())
                await writer.drain()

        except Exception as e:
            logging.error(f"Error handling transactions for client {client_address}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self):
        """Binds asynchronous socket servers and executes automatic discovery hooks."""
        # Initialize the non-blocking network socket infrastructure
        server = await asyncio.start_server(
            self.handle_client, 
            '127.0.0.1', 
            self.PORT,
            backlog=8192 # Expand kernel accept backlog queue to smoothly absorb connection bursts
        )
        logging.info(f"Storage Server active on port {self.PORT}")

        # --- High-Performance Asynchronous Server Registration Hook ---
        try:
            reg_reader, reg_writer = await asyncio.open_connection(self.LB_HOST, self.LB_PORT)
            reg_writer.write(f"REGISTER:{self.PORT}\n".encode())
            await reg_writer.drain()
            reg_writer.close()
            await reg_writer.wait_closed()
            logging.info(f"Discovery: Registered port {self.PORT} securely with Load Balancer.")
        except Exception as e:
            logging.error(f"Discovery Alert: Gateway registration handshake failed: {e}")
        # -------------------------------------------------------------

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    server_port = 12345
    if len(sys.argv) > 1:
        try:
            server_port = int(sys.argv[1])
        except ValueError:
            logging.warning("Invalid port provided, reverting to default 12345")

    try:
        asyncio.run(AsyncServer(port=server_port).start())
    except KeyboardInterrupt:
        logging.info("Storage node shutting down cleanly.")