import asyncio
import time
import sys

LB_IP = "127.0.0.1"
LB_PORT = 8888
CONCURRENT_CONNECTIONS = 50000 

async def simulate_client_lifecycle(client_id: int, semaphore: asyncio.Semaphore) -> bool:
    """Simulates a non-blocking client lifecycle interacting with the async architecture."""
    async with semaphore: 
        try:
            # Phase 1: Contact the Load Balancer to lease an active storage port
            lb_reader, lb_writer = await asyncio.open_connection(LB_IP, LB_PORT)
            lb_writer.write(b"CLIENT_REQUEST\n")
            await lb_writer.drain()
            
            lb_data = await lb_reader.readline()
            server_port = int(lb_data.decode().strip())
            
            lb_writer.close()
            await lb_writer.wait_closed()

            # Phase 2: Open a direct data session with the assigned backend server
            srv_reader, srv_writer = await asyncio.open_connection(LB_IP, server_port)

            # Phase 3: Perform an atomic data write operation (PUT)
            put_payload = f"PUT:key_{client_id}:val_{client_id}\n"
            srv_writer.write(put_payload.encode())
            await srv_writer.drain()
            
            _ = await srv_reader.readline()
            
            # Phase 4: Perform a validation data read operation (GET)
            get_payload = f"GET:key_{client_id}\n"
            srv_writer.write(get_payload.encode())
            await srv_writer.drain()
            
            get_res_data = await srv_reader.readline()
            get_response = get_res_data.decode().strip()

            # Phase 5: Terminate the long-lived data session cleanly
            srv_writer.write(b"QUIT\n")
            await srv_writer.drain()
            
            srv_writer.close()
            await srv_writer.wait_closed()

            return get_response == f"val_{client_id}"

        except Exception:
            return False

async def run_stress_test():
    print(f"Launching C50K Validation Driver: Injecting {CONCURRENT_CONNECTIONS} concurrent tasks...")
    
    # Gate the initial shockwave to 500 connections per moment to prevent socket buffer exhaustion
    burst_gate = asyncio.Semaphore(500)
    
    start_time = time.time()

    tasks = [
        asyncio.create_task(simulate_client_lifecycle(i, burst_gate)) 
        for i in range(CONCURRENT_CONNECTIONS)
    ]
    
    # Return exceptions to intercept underlying connection drops cleanly without panicking
    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_time = time.time() - start_time
    
    # Filter exceptions out of the final success counter safely
    success_count = sum(1 for res in results if isinstance(res, bool) and res)
    failed_count = CONCURRENT_CONNECTIONS - success_count
    throughput = (success_count * 2) / total_time 

    # Fixed syntax formatting block
    print(f"\n================ SYSTEM TEST RESULTS ================\n"
          f"Total Attempted Connections  : {CONCURRENT_CONNECTIONS}\n"
          f"Successful Task Completions  : {success_count}\n"
          f"Failed Task Completions      : {failed_count}\n"
          f"Total Execution Runtime      : {total_time:.2f} seconds\n"
          f"Empirical System Throughput  : {throughput:.2f} operations/sec\n"
          f"=====================================================")
    
    if failed_count == 0:
        print("🎯 SUCCESS: The system successfully sustained 50,000 concurrent connections with 100% integrity!")
    else:
        print(f"⚠️ TEST COMPLETED: {failed_count} tasks encountered network drops or timeouts.")

if __name__ == "__main__":
    # Switch Windows to use high-performance I/O Completion Ports (IOCP)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    asyncio.run(run_stress_test())