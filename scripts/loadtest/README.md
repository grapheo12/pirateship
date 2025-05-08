# Locust Load Testing

This document provides all the instructions you need to install and run load tests using [Locust](https://locust.io/). Locust is an open-source load testing tool that allows you to simulate user behavior in Python and generate load against your system.

## Installation

Install Locust using pip3 by running:

```bash
pip3 install locust

```

## Running Locust


Run all commands from inside the loadtest folder:

get_distribution: ratio of get_requests out of 100. For example if get_distribution = 80, then tests will run 80% get requests, 20% post requests.

```bash
source .venv/bin/activate
cd scripts_v2/loadtest
python3 loadtest.py http://localhost:8080 <NUMBER_OF_USERS> locustfile.py <GET_DISTRIBUTION>

```
