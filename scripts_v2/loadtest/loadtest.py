import json
import requests
import subprocess
import sys
import time

def register_users(host, num_users, password="pirateship"):
    max_user_id_length = len(str(num_users))
    
    for n in range(1, num_users + 1):
        username = "username" + (max_user_id_length - len(str(n))) * "0" + str(n)
        
        response = requests.post(
            f"{host}/register",
            json={"username": username, "password": password}
        )
        if response.status_code != 200:
            print(f"Error registering {username}: {response.text}")
        
        response = requests.post(
            f"{host}/refresh",
            json={"username": username, "password": password}
        )
        if response.status_code != 200:
            print(f"Error refreshing {username}: {response.text}")

def run_locust(locust_file, host, num_users, getDistribution):
    custom_user_config = {
        "user_class_name":"testClass", 
        "getDistribution": getDistribution
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
        print("Usage: python run_test.py <host> <num_users> <locust_file> <spawn_rate>")
        sys.exit(1)
    
    host = sys.argv[1]
    num_users = int(sys.argv[2])
    locust_file = sys.argv[3]
    spawn_rate = int(sys.argv[4])
    
    register_users(host, num_users)
    
    time.sleep(2)
    
    run_locust(locust_file, host, num_users, spawn_rate)
