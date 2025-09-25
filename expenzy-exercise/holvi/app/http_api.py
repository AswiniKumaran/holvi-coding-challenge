"""
This is the place to implement Holvi's integration!
"""
import os

from flask import Flask

from database import DBConnection
import requests
from urllib.parse import urljoin

app = Flask(__name__)

EXPENZY_API_BASE_URL = os.environ.get("EXPENZY_API_BASE_URL", "127.0.0.1")

@app.route("/expenzy/webhook/", methods=["GET"])
def expenzy_webhook():
    print("Webhook received")
    # Starting point for core logic - fetch transactions, store them in db, update
    # state to Expenzy, avoiding duplicates and missed ones.
    # 
    # *** To call an API:
    # response = requests.post(urljoin(EXPENZY_API_BASE_URL, '/api/transaction/?...'))
    # response.ok / response.raise_for_status()  # to check if response was successful
    # data = response.json()  # get response's json data convered to Python object
    #
    # *** To use database:
    # connection = create_database_connection()
    # query for multiple rows, one row, execute SQL which returns nothing
    # connection.fetch_results() / connection.fetch_one() / connection.execute()
    #
    # *** For concurrency control (python level locking can also work):
    # connection.lock() / connection.unlock()
    #
    # You can safely assume there's just one *process* running, but with *multiple threads*.
    #
    # If you want to simulate a worker process, then for example threading.Queue is a way
    # to simulate this. For example of usage see
    # https://docs.python.org/3/library/queue.html#queue.Queue.join
    return "ok"


@app.route("/payout/count", methods=["GET"])
def payout_count():
    """
    A small helper for db_check.py to fetch amount of recorded payouts.
    """
    (num_payouts,) = result = create_database_connection().fetch_one(
        "SELECT COUNT(*) FROM holvi_received_payout"
    )

    return str(num_payouts)


def create_database_connection():
    return DBConnection(
        hostname=os.environ.get("DB_HOSTNAME", "127.0.0.1"),
        username=os.environ.get("DB_USERNAME", "shared"),
        password=os.environ.get("DB_PASSWORD", "shared"),
        database=os.environ.get("DB_DATABASE", "shared"),
    )


if __name__ == "__main__":
    app.run(debug=True, host='holvi-api', port=5002)
