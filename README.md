# WIP: Running Experiments

## Setup script environment

```bash
virtualenv .venv
source .venv/bin/activate
pip install -r scripts_v2/requirements.txt
```

## Running end-to-end experiment flow

(From the virutalenv)

```bash
python3 scripts_v2/main.py -c scripts_v2/conf.toml
```

## Running stages separately

Check `python3 scripts_v2/main.py -h` for commands that can be invoked standalone.

# Current status

![Performance of Pirateship wrt other protocols; Non-TEE and LAN setup](perf.png)
