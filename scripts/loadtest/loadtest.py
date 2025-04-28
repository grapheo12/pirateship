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

async def create_bank_account(host, usernames, connector):
    try: 
        for username in usernames:
            async with connector.post(f"{host}/register", json={"username": username}) as response:
                if response.status != 200:
                    print(f"Error registering {username}: {await response.text()}")
    except Exception as e:
        await connector.close()
        print(f"An error occurred: {e}")

async def register_users(host, num_users, application, password="pirateship", workers_per_client=2, num_client_nodes=2, threshold=1):
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

        _usernames = ["user" + str(uuid.UUID(int=rnd.getrandbits(128))) for j in range(users_per_clients[i])]
        usernames.extend(_usernames)

    with open("Usernames.log", "w") as f:
        for username in usernames:
            f.write(username + "\n")
        

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




def run_locust(locust_file, host, num_users, getDistribution, getRequestHosts=[], master_host="localhost", workers_per_client=2, num_client_nodes=2):
    custom_user_config = {
        "user_class_name": "TestUser",   
        "getDistribution": getDistribution,
        "getRequestHosts": getRequestHosts,
        "workload": application,
    } 
    json_config = json.dumps(custom_user_config)
    json_config = "'" + json_config + "'"

    procs = []

    total_machines = workers_per_client * num_client_nodes
    users_per_clients = [num_users // total_machines] * total_machines
    users_per_clients[-1] += num_users - sum(users_per_clients)

    # Spawn the master node
    command = [
        "locust",
        "-f", locust_file,
        "--headless",
        "--users", str(num_users),
        "--spawn-rate", str(num_users),
        "-H", host,
        "--config-users", json_config,
        "--master",
    ]

    with open("master.log", "w") as master_log:
        cmd = " ".join(command)
        print("Starting Locust master with command:", cmd)
        proc = subprocess.Popen(cmd, stdout=master_log, stderr=subprocess.STDOUT, shell=True)
        procs.append(proc)


    for client_num in range(num_client_nodes):
        for worker_num in range(workers_per_client):
            with open(f"worker_{client_num}_{worker_num}.log", "w") as worker_log:
                cmd = command[:-2]
                custom_user_config["machineId"] = client_num * workers_per_client + worker_num
                custom_user_config["max_users"] = users_per_clients[client_num * workers_per_client + worker_num]
                json_config = json.dumps(custom_user_config)
                json_config = "'" + json_config + "'"
                cmd.extend([json_config, "--worker", "--master-host", master_host, "--processes", str(1)])

                cmd = " ".join(cmd)
                print("Starting Locust worker", custom_user_config["machineId"], "with command:", cmd)
                proc = subprocess.Popen(cmd, stdout=worker_log, stderr=subprocess.STDOUT, shell=True)
                procs.append(proc)


    # Wait for all to finish
    for proc in procs:
        proc.wait()
    



if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python run_test.py <host> <num_users> <locust_file> <get_ratio> <application> [<get_request_hosts>]")
        sys.exit(1)
    
    parser = argparse.ArgumentParser(description="Run load tests with locust")
    
    # Define all command-line arguments with '--' prefix for named parameters.
    parser.add_argument("--host", required=True)
    parser.add_argument("--num_users", type=int, required=True)
    parser.add_argument("--locust_file", required=True, help="Path to the locust file")
    parser.add_argument("--get_ratio", type=int, default=100, help="Ratio of GET requests")
    parser.add_argument("--application", required=True, choices=["kms", "svr3", "smallbank"])
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--get_request_hosts", nargs="+", default=[])
    #svr3 specific arguments
    parser.add_argument("--threshold", type=int, default=1)

    args = parser.parse_args()

    host = args.host
    num_users = args.num_users
    locust_file = args.locust_file
    get_ratio = args.get_ratio
    application = args.application
    nodes = args.nodes
    get_request_hosts = args.get_request_hosts
    threshold = args.threshold

    if application == "kms":
        print("Performing Load Phase with KMS...")
        asyncio.run(register_users(host, num_users, application))
        
        time.sleep(2)
    elif application == "svr3":
        print("Performing Load Phase with SVR3...")
        asyncio.run(register_users(host, num_users, application, threshold=threshold))
    elif application == "smallbank":
        print("performing load phase with smallbank")
        asyncio.run(register_users(host, num_users, application))


    print("Performing Run Phase...")
        
    run_locust(locust_file, host, num_users, get_ratio, get_request_hosts)

    print("Load test completed.")
