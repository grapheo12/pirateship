from locust import HttpUser, task, between, events
import requests, time

usernames = []
#this is synchronous with the load test.
@events.test_start.add_listener 
def on_test_start(environment):

    num_users = environment.parsed_options.num_users
    host = environment.parsed_options.host
    password = "pirateship"
    max_user_id_length = len(str(num_users))

    for n in range(1, num_users + 1):
        username = "username" + (max_user_id_length - len(str(n))) * "0" + str(n)
        usernames.append(username)

        response = requests.post(
            f"{host}/register",
            json={"username": username, "password": password}
        )


        response = requests.post(
            f"{host}/refresh",
            json={"username": username, "password": password}
        )
    


class testClass(HttpUser):
    users = []
    user_id = 0
    wait_time = between(1, 5)

    def on_start(self):
        self.username = usernames.pop(0)
        self.password = "pirateship"
          
    @task
    def task1(self):
        self.client.get("/pubkey", json={"username": self.username})

    





    



