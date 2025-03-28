from locust import HttpUser, task, between
import sys


class testClass(HttpUser):

    wait_time = between(1, 5)
    #read requests
    def listpubkeys(self, username):
        self.client.get("/listpubkeys", json={"username": username})

    def pubkey(self, username):
        self.client.get("/pubkey", json={"username": username})

    def privkey(self, username, password):
        self.client.get("/privkey", json={"username": username, "password": password})

    
    # write requests
    def register(self, username, password):
        self.client.post("/register", json={"username": username, "password": password})

    def refresh(self, username, password):
        self.client.post("/refresh", json={"username": username, "password": password})

    @task
    def task1(self):
        self.listpubkeys("teddy")
    
    @task
    def task2(self):
        self.client.get("")




    



