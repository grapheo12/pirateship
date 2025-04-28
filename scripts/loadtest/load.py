import json
import subprocess
import sys
import time
import asyncio
import aiohttp
import random
import uuid
import shamir
import argparse

MAX_CONCURRENT_REQUESTS = 200

RAND_SEED_LIST = [42, 120, 430, 82, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]

async def register_user(host, usernames, password, session):
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

async def create_secret(host, usernames, password, session, nodes, threshold):
    try:
        # secret = 123456789
        # splits = shamir.split_secret(secret, nodes, threshold, 10)
        for username in usernames:
            async with session.post(f"{host}/auth", json={"username": username, "pin": password}) as response:
                if response.status != 200:
                    print(f"Error registering {username}: {await response.text()}")

            # async with session.post(f"{host}/storesecret", json={"username": username, "password": password, "val": str(secret), "pin": "1234"}) as response:
            #     if response.status != 200:
            #         print(f"Error storing secret {username}: {await response.text()}")
    except Exception as e:
        await session.close()
        print(f"An error occurred: {e}")

async def create_bank_account(host, usernames, session):
    try: 
        for username in usernames:
            async with session.post(f"{host}/register", json={"username": username}) as response:
                if response.status != 200:
                    print(f"Error registering {username}: {await response.text()}")
    except Exception as e:
        await session.close()
        print(f"An error occurred: {e}")

async def register_users(host, num_users, application, password="pirateship", workers_per_client=2, num_client_nodes=1, threshold=1):
    max_user_id_length = len(str(num_users))

    tasks = []

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    total_machines = workers_per_client * num_client_nodes

    users_per_clients = [num_users // total_machines] * total_machines
    users_per_clients[-1] += num_users - sum(users_per_clients)

    usernames = []
    for i in range(total_machines):
        # Set a different random seed for each client node
        rnd = random.Random()
        rnd.seed(RAND_SEED_LIST[i % len(RAND_SEED_LIST)] * (1 + i // len(RAND_SEED_LIST)))

        _usernames = ["user" + str(uuid.UUID(int=rnd.getrandbits(128))) for _ in range(users_per_clients[i])]
        usernames.extend(_usernames)
        

    # Split the usernames into chunks to avoid overwhelming the server
    chunk_size = MAX_CONCURRENT_REQUESTS
    username_chunks = []
    for i in range(0, len(usernames), chunk_size):
        username_chunks.append(usernames[i:i + chunk_size])

    
    async with aiohttp.ClientSession(connector=connector) as session:
        for chunk in username_chunks:
            if application == "kms":
                tasks.append(register_user(host, chunk, password, session))
            if application == "svr3":
                tasks.append(create_secret(host, chunk, password, session, total_machines, threshold))
            if application == "smallbank":
                tasks.append(create_bank_account(host, chunk, session))

        await asyncio.gather(*tasks, return_exceptions=True)

    await connector.close()



if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python load.py <host> <num_users> <num_client_node> <workers_per_client> <application> <threshold>")
        sys.exit(1)
    
    host = sys.argv[1]
    num_users = int(sys.argv[2])
    num_client_nodes = int(sys.argv[3])
    workers_per_client = int(sys.argv[4])

    if len(sys.argv) >= 6:
        application = str(sys.argv[5])
    else:
        application = "kms"

    if application == "kms":
        password = "pirateship"
    else:
        password = "1234"


    # Threshold is not used
    if len(sys.argv) >= 7:
        threshold = int(sys.argv[6])
    else:
        threshold = 1

    
    print("Performing Load Phase...")
    asyncio.run(register_users(host, num_users, application, password=password, workers_per_client=workers_per_client, num_client_nodes=num_client_nodes, threshold=threshold))
