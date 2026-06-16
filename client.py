import asyncio
import sys
import logging

# Configure production-grade client logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Client-Engine: %(message)s"
)

class AsyncClient:
    def __init__(self, server_ip: str = "127.0.0.1", lb_port: int = 8888):
        self.SERVER_IP = server_ip
        self.LB_PORT = lb_port

    async def fetch_server_port(self) -> int:
        """Asynchronously contacts the load balancer to lease an active storage node port."""
        reader, writer = await asyncio.open_connection(self.SERVER_IP, self.LB_PORT)
        
        # Dispatch gateway broker handshake routing token
        writer.write(b"CLIENT_REQUEST\n")
        await writer.drain()
        
        data = await reader.readline()
        response = data.decode().strip()
        
        writer.close()
        await writer.wait_closed()
        
        if response.startswith("ERROR"):
            raise RuntimeError(response)
        return int(response)

    async def execute_transaction(self, op: str, key: str, value: str = "") -> str:
        """One-off transaction runner—used primarily by high-concurrency automated drivers."""
        try:
            server_port = await self.fetch_server_port()
            reader, writer = await asyncio.open_connection(self.SERVER_IP, server_port)
            
            # Pack transaction into a single atomic application frame
            payload = f"{op}:{key}:{value}\n" if op == "PUT" else f"{op}:{key}\n"
            writer.write(payload.encode())
            await writer.drain()
            
            data = await reader.readline()
            response = data.decode().strip()
            
            writer.write(b"QUIT\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return response
        except Exception as e:
            return f"TRANSACTION_ERROR: {e}"

    async def start_interactive_session(self):
        """Launches a long-lived, non-blocking interactive shell wrapper."""
        max_retries = 3
        reader, writer = None, None
        
        for attempt in range(max_retries):
            try:
                logging.info(f"Leasing target node from Load Balancer (Attempt {attempt + 1}/{max_retries})...")
                server_port = await self.fetch_server_port()
                
                logging.info(f"Opening direct persistent stream channel with node on port {server_port}...")
                reader, writer = await asyncio.open_connection(self.SERVER_IP, server_port)
                logging.info(f"Session successfully bonded with Storage Server Node: {server_port}")
                break
            except (asyncio.TimeoutError, ConnectionRefusedError, OSError, RuntimeError) as e:
                logging.warning(f"Node Recovery Loop: Server node unreached ({e}). Re-routing...")
                await asyncio.sleep(1)
        else:
            logging.error("Fatal Cluster Alert: Infrastructure completely unreachable.")
            return

        try:
            # Run loops using standard non-blocking system input lookups wrapped concurrently
            loop = asyncio.get_event_loop()
            while True:
                print("\nChoose operation (GET, PUT, REMOVE, DELETE) or 'QUIT' to exit:")
                # Offload blocking terminal input to an isolated executor to keep the event loop spinning
                operation = (await loop.run_in_executor(None, input)).upper().strip()
                
                if operation == "QUIT":
                    writer.write(b"QUIT\n")
                    await writer.drain()
                    break
                    
                if operation not in ["GET", "PUT", "REMOVE", "DELETE"]:
                    print("Invalid operation selection.")
                    continue
                
                print("Enter key:")
                key = (await loop.run_in_executor(None, input)).strip()
                
                # Compress properties down into a single application frame
                if operation == "PUT":
                    print("Enter value:")
                    value = (await loop.run_in_executor(None, input)).strip()
                    payload = f"PUT:{key}:{value}\n"
                else:
                    payload = f"{operation}:{key}\n"
                
                # Send atomic message payload downstream
                writer.write(payload.encode())
                await writer.drain()
                
                data = await reader.readline()
                print(f"Server response: {data.decode().strip()}")
                
        except Exception as e:
            logging.error(f"Fatal error during runtime data stream multiplexing: {e}")
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()
                logging.info("Client stream session disconnected cleanly.")

if __name__ == "__main__":
    client = AsyncClient()
    # Execute an interactive loop session safely inside the event loop engine context
    try:
        asyncio.run(client.start_interactive_session())
    except KeyboardInterrupt:
        logging.info("Interactive client engine exited cleanly.")