"""
This is the place to implement Holvi's integration!
"""
import os
from queue import Queue

from flask import Flask
import threading
from database import DBConnection
import requests
from urllib.parse import urljoin

from models import HolviReceivedPayout, HolviPayoutQuery
from time import sleep
import logging
logger = logging.getLogger(__name__)

max_retries = 10
app = Flask(__name__)
notifications_to_be_processed = Queue()
processed_transactions = Queue()
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
        logger.info("Payout has been fetched. Inserting them into the database.")
        for transaction in payout_data:
            self.payout_query.insert(connection=connection, payout = HolviReceivedPayout(
                expenzy_uuid=transaction['id'],
                amount=transaction['amount'],
                recipient_account_identifier=transaction['recipient_account_identifier'],
                create_time=transaction['create_time'],
            ))
            processed_transactions.put((transaction, 0))

    @staticmethod
    def update_status_to_expenzy_api(transaction):
        payload = {
            'state': 'processing',
        }
        response = requests.post(
            urljoin(EXPENZY_API_BASE_URL, f'/api/transaction/{transaction["id"]}/'),
            data=payload,
        )
        response.raise_for_status()

def handle_retry_mechanism(queue, task, current_attempt):
    logger.info(f"Retrying for {task} for {current_attempt} attempts")
    if current_attempt < max_retries:
        sleep(10)
        queue.put((task, current_attempt + 1))
    else:
        logger.error("Too many retries")
        return

def process_notification():
    while True:
        notification,attempts = notifications_to_be_processed.get()
        logger.info('Fetching payout data')
        handler = WebHookHandler(EXPENZY_API_BASE_URL)
        connection = None
        try:
            connection = create_database_connection()
            payout_data = handler.get_payout_data()
            payout_data = [i for i in payout_data if i['state'] == 'notifying']
            handler.process_payout_data_in_holvi(connection,payout_data)
            connection.commit_transaction()
            connection.close()
        except requests.exceptions.HTTPError as e:
            if "Server Error" in str(e) and "500" in str(e):
                logger.exception("Error while processing notification")
                handle_retry_mechanism(notifications_to_be_processed,notification,attempts)
            connection.rollback_transaction() if connection else None
        except Exception as e:
            connection.rollback_transaction() if connection else None
        finally:
            notifications_to_be_processed.task_done()
            connection.close() if connection else None

def update_transactions():
    while True:
        transaction,attempts = processed_transactions.get()
        logger.info(f"Processing transaction {transaction['id']}")
        try:
            WebHookHandler.update_status_to_expenzy_api(transaction)
        except requests.exceptions.HTTPError as e:
            if "Server Error" in str(e) and "500" in str(e):
                logger.exception(f"Error while updating transaction {transaction['id']}")
                handle_retry_mechanism(processed_transactions,transaction,attempts)
        except Exception as e:
            logger.exception(f"Error while updating transaction {transaction['id']}")
        finally:
            processed_transactions.task_done()

@app.route("/expenzy/webhook/", methods=["GET","POST"])
def expenzy_webhook():
    logger.info("Webhook received")
    notifications_to_be_processed.put(("Payout notication webhook received", 0))

    # Starting point for core logic - fetch transactions, store the m in db, update
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
