import asyncio
import logging
from typing import List

# Configure high-performance production-grade logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Systems-Engine: %(message)s"
)

class AsyncLoadBalancer:
    def __init__(self, lb_host: str = '127.0.0.1', lb_port: int = 8888):
        self.LB_HOST = lb_host
        self.LB_PORT = lb_port
        
        # State tracking array for running backend nodes
        self.server_addresses: List[int] = []
        self.current_index = 0
        
        # NOTE: At C50K scale, we eliminate threading.Lock entirely.
        # Since asyncio runs on a single-threaded event loop, standard race conditions 
        # from parallel mutations are completely impossible!

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Asynchronous, non-blocking connection broker handler loop."""
        client_address = writer.get_extra_info('peername')
        try:
            # Read incoming frames safely up to 1024 bytes without blocking the main event loop
            data = await reader.read(1024)
            if not data:
                return

            payload = data.decode().strip()

            # --- Dynamic Discovery Handler Block ---
            if payload.startswith("REGISTER:"):
                try:
                    server_port = int(payload.split(":")[1])
                    if server_port not in self.server_addresses:
                        self.server_addresses.append(server_port)
                        logging.info(f"Dynamic Discovery: Registered storage node on port {server_port}")
                except ValueError:
                    logging.warning("Malformed registration payload dropped.")
                return

            # --- Client Routing Handler Block ---
            if payload == "CLIENT_REQUEST":
                if not self.server_addresses:
                    writer.write(b"ERROR: No backend nodes available\n")
                    await writer.drain() # Non-blocking flush of network buffers
                    return
                
                # Execute Round-Robin scheduling algorithm via index pointer updates
                server_port = self.server_addresses[self.current_index]
                self.current_index = (self.current_index + 1) % len(self.server_addresses)
                
                # Fast connection broker response loop
                response = f"{server_port}\n".encode()
                writer.write(response)
                await writer.drain()
                
        except Exception as e:
            logging.error(f"Error handling routing metrics for client {client_address}: {e}")
        finally:
            writer.close()
            await writer.wait_closed() # Clean kernel socket descriptor teardown

    async def _health_check_loop(self):
        """Asynchronous active monitoring loop running out-of-band probes every 3 seconds."""
        while True:
            await asyncio.sleep(3) # Asynchronously yields control back to the event loop
            if not self.server_addresses:
                continue

            current_pool = list(self.server_addresses)
            for port in current_pool:
                try:
                    # Attempt a rapid connection probe without generating blocking OS threads
                    _, writer = await asyncio.open_connection(self.LB_HOST, port)
                    writer.close()
                    await writer.wait_closed()
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    logging.warning(f"Fault Detection: Node on port {port} dropped connection. Evicting.")
                    if port in self.server_addresses:
                        self.server_addresses.remove(port)
                        
                        # Boundary safety optimization loop for indexing pointers
                        if self.current_index >= len(self.server_addresses) and len(self.server_addresses) > 0:
                            self.current_index = 0

    async def start(self):
        """Initializes non-blocking socket loops and hooks into kernel event multiplexers."""
        # Spin up the active health checker as an asynchronous background concurrent task
        asyncio.create_task(self._health_check_loop())

        # Bind the network socket with integrated SO_REUSEADDR configurations embedded implicitly
        server = await asyncio.start_server(
            self.handle_client, 
            self.LB_HOST, 
            self.LB_PORT,
            backlog=8192 # Crucial: Expand OS queue backlog buffer to absorb connection storms
        )

        logging.info(f"Asynchronous Load Balancer active on {self.LB_HOST}:{self.LB_PORT}")
        
        async with server:
            await server.serve_forever() # Keep event loop listening indefinitely

if __name__ == "__main__":
    # Launch the asynchronous runtime context pipeline
    try:
        asyncio.run(AsyncLoadBalancer().start())
    except KeyboardInterrupt:
        logging.info("Load Balancer shut down cleanly.")