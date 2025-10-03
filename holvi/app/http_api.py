"""
This is the place to implement Holvi's integration!
"""
import datetime
import os
from queue import Queue
from typing import Union
from flask import Flask
import threading
import requests
from urllib.parse import urljoin

from database_operations import AddHolviTransaction, AddFailedTransaction, HandleDBOperation
from models import HolviPayoutQuery, FailedTransactionQuery
import logging

app = Flask(__name__)

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
QUEUE_MAXSIZE = int(os.environ.get("QUEUE_SIZE", 1000))

notifications_to_be_processed = Queue(maxsize=QUEUE_MAXSIZE)
processed_transactions = Queue(maxsize=QUEUE_MAXSIZE)

EXPENZY_API_BASE_URL = os.environ.get("EXPENZY_API_BASE_URL", "127.0.0.1")

class WebHookHandler:
    def __init__(self, expenzy_api_base_url: str):
        self.expenzy_api_base_url = expenzy_api_base_url
        self.insert_data_to_holvi = AddHolviTransaction(HolviPayoutQuery())
        self.insert_failed_transaction = AddFailedTransaction(FailedTransactionQuery())

    def get_payout_data(self):
        response = requests.post(urljoin(self.expenzy_api_base_url, '/api/transaction/'))
        response.raise_for_status()  # to check if response was successful
        return response.json()  # get response's json data converted to Python object

    def process_payout_data_in_holvi(self, payout_data: list[dict]):
        logger.info("Payout has been fetched. Inserting them into the database.")
        for transaction in payout_data:
            self.insert_data_to_holvi.perform_db_operation(transaction)
            processed_transactions.put((transaction, 1))

    def update_status_to_expenzy_api(self, transaction: dict):
        payload = {
            'state': 'processing',
        }
        response = requests.post(
            urljoin(self.expenzy_api_base_url, f'/api/transaction/{transaction["id"]}/'),
            data=payload,
        )
        response.raise_for_status()

    def write_failed_transaction_to_db(self, transaction: dict, attempted_at: datetime.datetime):
        self.insert_failed_transaction.perform_db_operation(transaction, attempted_at)


def handle_retry_mechanism(queue: Queue, task: Union[str, dict], current_attempt: int, handler: WebHookHandler,
                           persist_to_db: bool = False):
    delay = 5
    logger.info(f"Retrying for {task} for {current_attempt} attempts")
    if current_attempt <= MAX_RETRIES:
        threading.Timer(delay * (2 ** current_attempt), lambda: queue.put((task, current_attempt + 1))).start()
    else:
        logger.error("Too many retries")
        if persist_to_db:
            handler.write_failed_transaction_to_db(task, datetime.datetime.now())
        return


def process_notification():
    handler = WebHookHandler(EXPENZY_API_BASE_URL)
    while True:
        notification, attempts = notifications_to_be_processed.get()
        logger.info('Fetching payout data')
        try:
            payout_data = handler.get_payout_data()
            payout_data = [i for i in payout_data if i['state'] == 'notifying']
            handler.process_payout_data_in_holvi(payout_data)
        except requests.exceptions.HTTPError as e:
            if "Server Error" in str(e) and "500" in str(e):
                logger.exception("Server Error while processing notification")
                handle_retry_mechanism(notifications_to_be_processed, notification, attempts, handler)
        except Exception as e:
            logger.exception("Unexpected Error while processing notification")
        finally:
            notifications_to_be_processed.task_done()


def update_transactions():
    handler = WebHookHandler(EXPENZY_API_BASE_URL)
    while True:
        transaction, attempts = processed_transactions.get()
        logger.info(f"Processing transaction {transaction['id']}")
        try:
            handler.update_status_to_expenzy_api(transaction)
        except requests.exceptions.HTTPError as e:
            if "Server Error" in str(e) and "500" in str(e):
                logger.exception(f"Error while updating transaction {transaction['id']}")
                handle_retry_mechanism(processed_transactions, transaction, attempts, handler, persist_to_db=True)
        except Exception as e:
            logger.exception(f"Error while updating transaction {transaction['id']}")
        finally:
            processed_transactions.task_done()


@app.route("/expenzy/webhook/", methods=["GET", "POST"])
def expenzy_webhook():
    logger.info("Webhook received")
    notifications_to_be_processed.put(("Payout notication webhook received", 1))
    return "ok"


@app.route("/payout/count", methods=["GET"])
def payout_count():
    """
    A small helper for db_check.py to fetch amount of recorded payouts.
    """
    with HandleDBOperation.connection_pool.connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM holvi_received_payout")
            result = cursor.fetchone()
            (num_payouts,) = result

    return str(num_payouts)


if __name__ == "__main__":
    HandleDBOperation.setup_db_connection_pool()
    threading.Thread(target=process_notification, daemon=True).start()
    threading.Thread(target=update_transactions, daemon=True).start()
    app.run(debug=True, host='holvi-api', port=5002)
