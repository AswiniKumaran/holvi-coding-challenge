import json
import queue
import unittest
import uuid
from unittest.mock import patch
import datetime
from http_api import app, WebHookHandler, notifications_to_be_processed, processed_transactions, handle_retry_mechanism


class TestWebHookHandler(unittest.TestCase):

    def setUp(self):
        self.test_id = uuid.uuid4()
        self.client = app.test_client()
        self.test_notifications_queue = queue.Queue()
        self.test_transactions_queue = queue.Queue()
        notifications_to_be_processed.queue = self.test_notifications_queue
        processed_transactions.queue = self.test_transactions_queue
        self.handler = WebHookHandler('http://fake-api')

    def get_test_data(self):
        return [{
            "id": str(self.test_id),
            "amount": 100,
            "recipient_account_identifier": "acc_001",
            "create_time": datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "state": "notifying"
        }]

    def test_webhook_notfication_received(self):
        response = self.client.post('/expenzy/webhook/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.decode(), "ok")
        notification, attempts = self.test_notifications_queue.get()
        self.assertEqual(notification, "Payout notication webhook received")
        self.assertEqual(attempts, 1)

    @patch('database_operations.AddHolviTransaction.perform_db_operation')
    def test_db_insert_was_called(self, mock_insert):
        fake_payout = self.get_test_data()
        self.handler.process_payout_data_in_holvi(fake_payout)
        self.assertTrue(mock_insert.called)
        args, kwargs = mock_insert.call_args
        transaction_serialized = args[0]
        self.assertEqual(transaction_serialized.expenzy_uuid, self.test_id)
        self.assertEqual(transaction_serialized.amount, 100)
        self.assertEqual(transaction_serialized.recipient_account_identifier, "acc_001")

    @patch('database_operations.AddHolviTransaction.perform_db_operation')
    @patch.object(WebHookHandler, 'get_payout_data')
    def test_update_transaction(self, mock_get_payout_data, mock_db_insert):
        self.test_notifications_queue.put(("Payout notication webhook received", 1))
        self.client.post('/expenzy/webhook/', data=json.dumps(self.get_test_data()), content_type='application/json')
        self.handler.process_payout_data_in_holvi(self.get_test_data())
        transaction_id, _ = self.test_transactions_queue.get()
        self.assertEqual(transaction_id, self.test_id)

    @patch('http_api.requests.post')
    def test_update_status_to_expenzy_api(self, mock_post):
        self.handler.update_status_to_expenzy_api(self.test_id)
        mock_post.assert_called_once()
        called_url = mock_post.call_args[0][0]
        self.assertIn(str(self.test_id), called_url)

    @patch('database_operations.AddFailedTransaction.perform_db_operation')
    def test_failed_transaction_written(self, mock_failed_db):
        handle_retry_mechanism(self.test_transactions_queue, self.test_id, 10, self.handler, persist_to_db=True)
        mock_failed_db.assert_called()
