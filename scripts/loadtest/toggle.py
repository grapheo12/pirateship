import datetime
import requests
from sys import argv
from time import sleep
import logging

logger = logging.getLogger(__name__)

def toggle_byz_commit(host, ramp_up, duration):
    logger.info(f"Waiting {ramp_up} seconds")
    sleep(ramp_up)
    try:
        logger.info("Toggling byz commit")
        resp = requests.post(f"{host}/toggle_byz_wait")
        if resp.status_code != 200:
            logger.error(f"Failed to toggle byz commit: {resp.status_code}")
            return
        logger.info(f"Byz commit toggled: {resp.json()}")
    except Exception as e:
        logger.error(f"Failed to toggle byz commit: {e}")
        return
    
    logger.info(f"Toggled byz commit waiting for {duration} seconds")
    sleep(duration)

    try:
        logger.info("Toggling byz commit back")
        resp = requests.post(f"{host}/toggle_byz_wait")
        if resp.status_code != 200:
            logger.error(f"Failed to toggle byz commit: {resp.status_code}")
            return
        logger.info(f"Byz commit toggled back: {resp.json()}")
    except Exception as e:
        logger.error(f"Failed to toggle byz commit: {e}")
        return

    logger.info("Toggle end")



if __name__ == "__main__":
    if len(argv) != 4:
        print("Usage: python toggle.py <host> <ramp_up> <duration>")
        exit(1)

    host = argv[1]
    ramp_up = int(argv[2])
    duration = int(argv[3])

    # Follow the same logging format as log4rs config.
    logging.Formatter.formatTime = (lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc).astimezone().isoformat(sep="T",timespec="milliseconds"))
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s][%(module)s][%(asctime)s] %(message)s')


    toggle_byz_commit(host, ramp_up, duration)
