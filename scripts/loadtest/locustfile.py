from asyncio.log import logger
import random
from locust import FastHttpUser, task, between, events
import requests, time, collections
import uuid
import random
import hashlib
import json
from pprint import pprint
from shamir import split_secret, reconstruct_secret
from zipfian import zipfian_sample
import pickle
import base64

# PRIME = 711577331239842805114550041213069154795634469703966193517588669628016485152750041731176583762267136103285795860447322461808709838439645383515985384043692155718147949524715956189033281228535852472381177902070093705760092109751916910892196462236676848628418360675233448440819174569075425896999465165453
PRIME = 42083
getWeight = 100
getRequestHosts = []
secretKeyHosts = []
threshold = len(getRequestHosts)
valid_usernames = [""]
active_users = set()



# This must be same between this file and loadtest.py
RAND_SEED_LIST = [42, 120, 430, 82, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
rnd = random.Random()

curr_num_users = 0
max_users = 1
glob_seed = None
workload = None


#setup phase -- this is synchronous with the load test.
@events.test_start.add_listener
def on_test_setup(environment, **kwargs):
    global getWeight, getRequestHosts, glob_seed, max_users, curr_num_users, rnd, threshold, workload

    user_config = environment.parsed_options.config_users[0]

    try:
        if type(user_config) == str:
            user_config = json.loads(user_config)
        else:
            user_config = environment.parsed_options.config_users[0][0]
        logger.info(user_config)
        getWeight = user_config["getDistribution"]
        print("getDistribution: ", getWeight, "postDistribution: ", 100 - getWeight)

        if not (0 <= getWeight <= 100):
            getWeight = 100
            print("set getDistribution in range of [0, 100], defaulting to only get requests")

        getRequestHosts = user_config.get("getRequestHosts", [])

        if "max_users" in user_config:
            max_users = int(user_config["max_users"])

        machineId = user_config.get("machineId", 0)
        print("my machineId: ", machineId)

        workload = user_config.get("workload", "kms")

        if workload == "svr3":
            threshold = user_config.get("threshold", len(getRequestHosts))

        

    except Exception as e:
        raise Exception(environment.parsed_options.config_users)
        # raise e

    # This ensures the same seed is used between load and run phase to generate user names
    seed = RAND_SEED_LIST[machineId % len(RAND_SEED_LIST)] * (1 + machineId // len(RAND_SEED_LIST))
    rnd.seed(seed)
    glob_seed = seed

    for i in range(max_users):
        valid_usernames.append("user" + str(uuid.UUID(int=rnd.getrandbits(128))))

    # print("valid_usernames: ", valid_usernames)


class TestUser(FastHttpUser):
    # wait_time = constant_throughput(50)

    def on_start(self):
        global getWeight, getRequestHosts, glob_seed, max_users, curr_num_users, rnd, workload, active_users

        if curr_num_users >= max_users:
            curr_num_users = 0
            rnd.seed(glob_seed)
        
        curr_num_users += 1

        # We need to magically generate unique usernames without any kind of central coordination.
        # Since with distributed load testing, we can't guarantee that the same username won't be used by another user.
        self.username = valid_usernames[curr_num_users] # "user" + str(uuid.UUID(int=rnd.getrandbits(128)))
        
        if workload == "kms":
            self.password = "pirateship"
        else:
            self.password = "1234"

        # For optimal load balancing, need to hash the username to a get_host
        hsh = hashlib.sha256(self.username.encode()).digest()
        val = int.from_bytes(hsh, 'big')

        if len(getRequestHosts) == 0:
            self.my_get_host = "" # Use the default host
        else:
            self.my_get_host = getRequestHosts[val % len(getRequestHosts)]

        if workload == "svr3":
            self.secretKeyHosts = [getRequestHosts[i] for i in random.sample(range(len(getRequestHosts)), k=threshold)]
            self.allHosts = getRequestHosts[:]
            logger.info(f"{threshold}, {self.secretKeyHosts}")

            self.rng = random.Random()

            self.current_secret = None

        if self.username in active_users:
            self.active = False
        else:
            self.active = True
            active_users.add(self.username)


        self.constant_secret = 112
        self.constant_shares = split_secret(self.constant_secret, len(getRequestHosts), len(getRequestHosts), PRIME)


    def on_stop(self):
        global active_users
        if self.username in active_users:
            active_users.remove(self.username)


    #run phase 
    @task
    def task1(self):
        global workload

        assert workload is not None

        if not self.active:
            return
        
        if workload == "kms":
            self.kms_task()
        if workload == "svr3":
            self.svr3_task()
        if workload == "smallbank":
            self.smallbank_task()

    def kms_task(self):
        choice = random.uniform(0, 100)
        if choice < getWeight:
            self.client.get(f"{self.my_get_host}/pubkey", json={"username": self.username})
        else:
            self.client.post("/refresh", json={"username": self.username, "password": "pirateship"})


    def svr3_task(self):
        choice = random.uniform(0, 100)
        if choice < getWeight:
            self.pin_guess(self.password)
        else:
            wrong_pin = "2341"
            self.pin_guess(wrong_pin)

        # if choice < getWeight and self.current_secret is not None:
        #     self.retrieve_secret_flow()
        # else:
        #     self.create_new_secret_flow()


    def pin_guess(self, pin):
        self.client.get("/gettoken", json={"username": self.username, "pin": pin, "increment_version": False})

    def smallbank_task(self):
        self.send_payment()
    
    def send_payment(self):
        while True:
            receiver = valid_usernames[random.sample(range(1, max_users), 1)[0]]
            if receiver != self.username:
                break
        send_amount = zipfian_sample(1, 10000, s=0.99, size=1)[0]
        # logger.info(f"RECIEVER {receiver}, VALID USERNAMES: {valid_usernames}")
        # logger.info(f"SEND AMOUNT {send_amount}")
        resp = self.client.post("/sendpayment", json={"sender_account":self.username, "receiver_account":receiver, "send_amount":send_amount})
        if resp.status_code == 200 and "send attempts" in resp.json():
            if resp.json()["send attempts"] > 1:
                logger.info(f"Multiple aborts: {resp.json()}")

    def create_new_secret_flow(self):
        # self.current_secret = self.rng.randint(0, (1 << 256) - 1)
        # hsh = hashlib.sha256(self.username.encode()).digest()
        # val = int.from_bytes(hsh, 'big')
        self.current_secret = self.constant_secret
        # Making this deterministic for testing

        t = len(self.secretKeyHosts)
        n = len(self.allHosts)

        # Shamir secret sharing
        shares = self.constant_shares[:]

        resp = self.client.get("/gettoken", json={"username": self.username, "pin": self.password, "increment_version": True})
        token = resp.json()

        if not "valid_until" in token:
            logger.warning(f"Token not valid for {self.username} {self.password}: {token}")
            return
        else:
            # print(f"Token: {token} Password: {self.password}")
            pass
        

        for i in range(n):
            host = self.allHosts[i]
            share = shares[i]

            val = pickle.dumps(share)
            val = base64.b64encode(val).decode('utf-8')
            payload = {
                "val": val,
                "token": token
            }
            resp = self.client.post(f"{host}/storesecret", json=payload)
            if resp.status_code != 200:
                logger.warning(f"Error storing secret {self.username}: {resp.text}")

    def retrieve_secret_flow(self):
        # Retrieve the shares from the secretKeyHosts
        resp = self.client.get("/gettoken", json={"username": self.username, "pin": self.password, "increment_version": False})
        token = resp.json()
        if not "valid_until" in token:
            logger.warning(f"Token not valid for {self.username} {self.password}: {token}")
            return
        else:
            # print(f"Token: {token} Password: {self.password}")
            pass
        
        shares = []
        for host in self.secretKeyHosts:
            resp = self.client.get(f"{host}/recoversecret", json=token)
            resp = resp.json()
            if "user secret" in resp:
                share = resp["user secret"]
                # Decode the base64 encoded share
                share = base64.b64decode(share)
                # Deserialize the share
                share = pickle.loads(share)
                shares.append(share)
            else:
                logger.warning(f"Error retrieving secret {self.username}: {resp}")

        # Reconstruct the secret using the shares
        t = len(self.secretKeyHosts)
        if len(shares) < t:
            logger.warning("Not enough secrets")
            return
        
        # Manually checked that this never fails.
        # Disabling for now.

        # secret = reconstruct_secret(shares, PRIME)

        # assert secret == self.current_secret, f"Secret mismatch: {secret} != {self.current_secret}"

        


    



