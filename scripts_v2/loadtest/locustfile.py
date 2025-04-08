import random
from locust import FastHttpUser, task, between, events, constant_throughput
import requests, time, collections
import uuid
import random
import hashlib
import json
from pprint import pprint

getWeight = 100
getRequestHosts = []



# This must be same between this file and loadtest.py
RAND_SEED_LIST = [42, 120, 430, 82, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
rnd = random.Random()

curr_num_users = 0
max_users = 1
glob_seed = None


#setup phase -- this is synchronous with the load test.
@events.test_start.add_listener
def on_test_setup(environment, **kwargs):
    global getWeight, getRequestHosts, glob_seed, max_users, curr_num_users, rnd

    user_config = environment.parsed_options.config_users[0]

    try:
        if type(user_config) == str:
            user_config = json.loads(user_config)

        getWeight = user_config["getDistribution"]
        print("getDistribution: ", getWeight, "postDistribution: ", 100 - getWeight)

        if not (0 <= getWeight <= 100):
            getWeight = 100
            postWeight = 0
            print("set getDistribution in range of [0, 100]")

        getRequestHosts = user_config.get("getRequestHosts", [])

        if "max_users" in user_config:
            max_users = int(user_config["max_users"])

        machineId = user_config.get("machineId", 0)
    except Exception as e:
        raise Exception(environment.parsed_options.config_users)
        # raise e

    # This ensures the same seed is used between load and run phase to generate user names
    seed = RAND_SEED_LIST[machineId % len(RAND_SEED_LIST)] * (1 + machineId // len(RAND_SEED_LIST))
    rnd.seed(seed)
    glob_seed = seed




class testClass(FastHttpUser):
    wait_time = constant_throughput(50)

    def on_start(self):
        global getWeight, getRequestHosts, glob_seed, max_users, curr_num_users, rnd

        if curr_num_users >= max_users:
            curr_num_users = 0
            rnd.seed(glob_seed)
        
        curr_num_users += 1

        # We need to magically generate unique usernames without any kind of central coordination.
        # Since with distributed load testing, we can't guarantee that the same username won't be used by another user.
        self.username = "user" + str(uuid.UUID(int=rnd.getrandbits(128)))
        self.password = "pirateship"


        # For optimal load balancing, need to hash the username to a get_host
        hsh = hashlib.sha256(self.username.encode()).digest()
        val = int.from_bytes(hsh, 'big')

        if len(getRequestHosts) == 0:
            self.my_get_host = "" # Use the default host
        else:
            self.my_get_host = getRequestHosts[val % len(getRequestHosts)]


    #run phase 
    @task
    def task1(self):
        choice = random.uniform(0, 100)
        if choice < getWeight:
            self.client.get(f"{self.my_get_host}/pubkey", json={"username": self.username})
        else:
            self.client.post("/refresh", json={"username": self.username, "password": "pirateship"})





    



