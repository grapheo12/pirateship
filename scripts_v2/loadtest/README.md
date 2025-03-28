# Locust Load Testing

This document provides all the instructions you need to install and run load tests using [Locust](https://locust.io/). Locust is an open-source load testing tool that allows you to simulate user behavior in Python and generate load against your system.

## Installation

Install Locust using pip3 by running:

```bash
pip3 install locust
```

## Running Locust

There are two main ways to run your load tests with Locust: using a configuration file or directly via command line arguments.

Run all commands from inside the loadtest folder:
```bash
cd scripts_v2/loadtest
```

### Option 1: Using a Configuration File

You can create a configuration file (e.g., `locust.conf`) to define your test parameters. Locust also detects locustfiles and config files, but specifying it is more clear.

Run Locust with:

```bash
locust -f locustfile.py --config locust.conf
```



### Option 2: Using Command Line Arguments

You can also run Locust directly by specifying the test parameters on the command line.

```bash
locust --headless --users 10 --spawn-rate 1 -H http://localhost:8080 -f locustfile.py
```
