"""
This is the place to implement Holvi's integration!
"""
import os
import queue

from flask import Flask
import threading
from database import DBConnection
import requests
from urllib.parse import urljoin

from models import HolviReceivedPayout, HolviPayoutQuery

app = Flask(__name__)
notifications_to_be_processed = queue.Queue()
processed_transactions = queue.Queue()
EXPENZY_API_BASE_URL = os.environ.get("EXPENZY_API_BASE_URL", "127.0.0.1")

def create_database_connection():
    return DBConnection(
        hostname=os.environ.get("DB_HOSTNAME", "127.0.0.1"),
        username=os.environ.get("DB_USERNAME", "shared"),
        password=os.environ.get("DB_PASSWORD", "shared"),
        database=os.environ.get("DB_DATABASE", "shared"),
    )

class WebHookHandler:
    payout_query = HolviPayoutQuery()

    def __init__(self, expenzy_api_base_url):
        self.expenzy_api_base_url = expenzy_api_base_url

    def get_payout_data(self):
        response = requests.post(urljoin(self.expenzy_api_base_url, '/api/transaction/'))
        response.raise_for_status()  # to check if response was successful
        return response.json()  # get response's json data convered to Python object

    def process_payout_data_in_holvi(self, connection, payout_data):
        for transaction in payout_data:
            self.payout_query.insert(connection=connection, payout = HolviReceivedPayout(
                expenzy_uuid=transaction['id'],
                amount=transaction['amount'],
                recipient_account_identifier=transaction['recipient_account_identifier'],
                create_time=transaction['create_time'],
            ))
            processed_transactions.put(transaction)

    @staticmethod
    def update_status_to_expenzy_api(transaction):
        payload = {
            'state': 'processing',
        }
        response = requests.post(
            urljoin(EXPENZY_API_BASE_URL, f'/api/transaction/{transaction["id"]}/'),
            data=payload,
        )
        print(response.json())
        response.raise_for_status()


def process_notification():
    connection = create_database_connection()
    while True:
        notification = notifications_to_be_processed.get()
        try:
            handler = WebHookHandler(EXPENZY_API_BASE_URL)
            payout_data = handler.get_payout_data()
            payout_data = [i for i in payout_data if i['state'] == 'notifying']
            handler.process_payout_data_in_holvi(connection,payout_data)
            connection.commit_transaction()
        except Exception as e:
            print(e)
            connection.rollback_transaction()
        finally:
            notifications_to_be_processed.task_done()

def update_transactions():
    while True:
        try:
            transaction = processed_transactions.get()
            WebHookHandler.update_status_to_expenzy_api(transaction)
        except Exception as e:
            print(e)
        finally:
            processed_transactions.task_done()

@app.route("/expenzy/webhook/", methods=["GET","POST"])
def expenzy_webhook():
    print("Webhook received")
    notifications_to_be_processed.put("Payout notication webhook received")

    # Starting point for core logic - fetch transactions, store them in db, update
    # state to Expenzy, avoiding duplicates and missed ones.
    # 
    # *** To call an API:

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



if __name__ == "__main__":
    threading.Thread(target=process_notification, daemon=True).start()
    threading.Thread(target=update_transactions, daemon=True).start()
    app.run(debug=True, host='holvi-api', port=5002)
