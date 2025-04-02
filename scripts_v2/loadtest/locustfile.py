from locust import HttpUser, task, between, events
import requests, time, collections

#queue storing usernames
usernames = collections.deque([])

#setup phase -- this is synchronous with the load test.
@events.test_start.add_listener 
def on_test_start(environment):

    num_users = environment.parsed_options.num_users
    max_user_id_length = len(str(num_users))
    for i in range(1, num_users + 1):
        usernames.append("username" + (max_user_id_length - len(str(i))) * "0" + str(i))
        
    print("USERNAMES" , usernames)

class testClass(HttpUser):
    user_id = 0

    def on_start(self):
        self.username = usernames.popleft() #client consumes from username queue to be assigned a username
        self.password = "pirateship"

    #run phase 
    @task(2)
    def task1(self):
        self.client.get("/pubkey", json={"username": self.username})

    @task(2)
    def task2(self):
        self.client.post("/refresh", json={"username": self.username, "password": "pirateship"})

    





    



