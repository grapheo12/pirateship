import json
import requests
import subprocess
import sys
import time
import asyncio
import aiohttp

MAX_CONCURRENT_REQUESTS = 200

async def register_user(host, usernames, password, connector):
    async with aiohttp.ClientSession(connector=connector) as session:
        try: 
            for username in usernames:
                async with session.post(f"{host}/register", json={"username": username, "password": password}) as response:
                    if response.status != 200:
                        print(f"Error registering {username}: {await response.text()}")

                async with session.post(f"{host}/refresh", json={"username": username, "password": password}) as response:
                    if response.status != 200:
                        print(f"Error refreshing {username}: {await response.text()}")
        except Exception as e:
            await session.close()
            print(f"An error occurred: {e}")

async def register_users(host, num_users, password="pirateship"):
    max_user_id_length = len(str(num_users))

    tasks = []

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    usernames = ["username" + (max_user_id_length - len(str(n))) * "0" + str(n) for n in range(1, num_users + 1)]
    # Split the usernames into chunks to avoid overwhelming the server
    chunk_size = MAX_CONCURRENT_REQUESTS
    username_chunks = []
    for i in range(0, len(usernames), chunk_size):
        username_chunks.append(usernames[i:i + chunk_size])

    
    for chunk in username_chunks:
        tasks.append(register_user(host, chunk, password, connector))

    await asyncio.gather(*tasks)

    await connector.close()



def run_locust(locust_file, host, num_users, getDistribution, getRequestHosts=[]):
    custom_user_config = {
        "user_class_name":"testClass", 
        "getDistribution": getDistribution,
        "getRequestHosts": getRequestHosts
    }
    json_config = json.dumps(custom_user_config)



    command = [
        "locust",
        "-f", locust_file,
        "--headless",
        "--users", str(num_users),
        "--spawn-rate", str(num_users),
        "-H", host,
        "--config-users",json_config
    ]
    
    print("Starting Locust with command:", " ".join(command))
    subprocess.run(command)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python run_test.py <host> <num_users> <locust_file> <get_ratio> [<get_request_hosts>]")
        sys.exit(1)
    
    host = sys.argv[1]
    num_users = int(sys.argv[2])
    locust_file = sys.argv[3]
    get_ratio = int(sys.argv[4])
    get_request_hosts = sys.argv[5:]

    
    print("Performing Load Phase...")
    asyncio.run(register_users(host, num_users))
    
    time.sleep(2)

    print("Performing Run Phase...")
    
    run_locust(locust_file, host, num_users, get_ratio, get_request_hosts)

    print("Load test completed.")
