import random
from locust import FastHttpUser, task, between, events
import requests, time, collections

#queue storing usernames
usernames = collections.deque([])

getWeight = 100

#setup phase -- this is synchronous with the load test.
@events.test_start.add_listener
def on_test_setup(environment, **kwargs):
    global getWeight

    user_config = environment.parsed_options.config_users[0][0]
    getWeight = user_config["getDistribution"]
    print("getDistribution: ", getWeight, "postDistribution: ", 100 - getWeight)

    if not (0 <= getWeight <= 100):
        getWeight = 100
        postWeight = 0
        print("set getDistribution in range of [0, 100]")
    
    num_users = environment.parsed_options.num_users
    max_user_id_length = len(str(num_users))
    for i in range(1, num_users + 1):
        usernames.append("username" + (max_user_id_length - len(str(i))) * "0" + str(i)) 

class testClass(FastHttpUser):
    user_id = 0
    GET_REQUEST_HOSTS = ["http://localhost:4001", "http://localhost:4002", "http://localhost:4003", "http://localhost:4004"]

    def on_start(self):
        self.username = usernames.popleft() #client consumes from username queue to be assigned a username
        self.password = "pirateship"

        self.my_get_host = self.GET_REQUEST_HOSTS[int(self.username[-1]) % len(self.GET_REQUEST_HOSTS)]
        # # Generate a random number in the range [0, total)
        # choice = random.uniform(0, 100)
        # if choice < getWeight:
        #     self.client.get("/pubkey", json={"username": "test_user"})
        # else:
        #     self.client.post("/refresh", json={"username": "test_user", "password": "pirateship"})

    #run phase 
    @task
    def task1(self):
        choice = random.uniform(0, 100)
        if choice < getWeight:
            self.client.get(f"{self.my_get_host}/pubkey", json={"username": self.username})
        else:
            self.client.post("/refresh", json={"username": self.username, "password": "pirateship"})





    



