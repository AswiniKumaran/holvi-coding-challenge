"""
This is a quickly written script which generates payouts and sends
webhook to partner about payouts.

As a bonus exercise you might want to spot cases violating best
practices in this script.
"""


from multiprocessing.pool import ThreadPool
import flask
import os
import requests
import traceback
from time import sleep
from urllib.parse import urljoin
from models import Payout, PayoutQuery
from database import DBConnection


HOLVI_API_BASE_URL = os.environ.get(
    "HOLVI_API_BASE_URL", "http://127.0.0.1:5002"
)


def notify_partner():
    try:
        response = requests.get(urljoin(HOLVI_API_BASE_URL, "expenzy/webhook/"))
        response.raise_for_status()
    except Exception as exc:
        traceback.print_exception(exc)


def generate_new_payout(connection):
    payout = Payout()
    PayoutQuery().insert(connection, payout)
    return payout


def main_loop():
    # The notifications are sent asynchronously and concurrently
    pool = ThreadPool(processes=int(os.getenv("CONCURRENCY", 2)))
    connection = DBConnection(
        hostname=os.environ.get("DB_HOSTNAME", "127.0.0.1")
    )
    num_attempts = 0
    while True:
        try:
            connection.begin_transaction()
            payout = generate_new_payout(connection)
            connection.commit_transaction()
            result = pool.apply_async(notify_partner)
        except Exception as exc:
            traceback.print_exception(exc)
        num_attempts += 1
        if num_attempts == int(os.getenv("GENERATION_ATTEMPTS", 10)):
            print("Done generation, bye!")
            break
        sleep(float(os.getenv("SLEEP_BETWEEN_PAYOUT", 0.1)))
    connection.close()
    pool.close()
    pool.join()


if __name__ == "__main__":
    main_loop()
