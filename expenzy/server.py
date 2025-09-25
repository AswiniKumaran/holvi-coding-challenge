from flask import Flask, request, jsonify
from database import DBConnection
from models import PayoutQuery
from dataclasses import asdict
import os
import random


DB_HOSTNAME = hostname = os.environ.get("DB_HOSTNAME", "127.0.0.1")

app = Flask(__name__)


@app.route("/api/transaction/", methods=["POST"])
def transaction_list():
    connection = DBConnection(DB_HOSTNAME)
    try:
        state = request.args.get("state")
        payouts = PayoutQuery().fetch(connection, state)
        return jsonify([asdict(p) for p in payouts])
    finally:
        connection.close()


@app.route("/api/transaction/<uuid:uuid>/", methods=["POST"])
def transaction_update(uuid):
    if random.random() < float(os.getenv("EXPENZY_FAILURE_RATE", 0.05)):
        raise Exception("This API sometimes fails")
    state = request.form.get("state")
    if state not in ("processing", "error"):
        return jsonify({'error': f'State {state} not in processing, error'}), 404
    connection = DBConnection(DB_HOSTNAME)
    try:
        payouts = PayoutQuery().update_state_by_id(connection, state, uuid)
        return jsonify([asdict(p) for p in payouts])
    finally:
        connection.close()


@app.route("/api/transaction/count", methods=["GET"])
def transaction_count():
    connection = DBConnection(DB_HOSTNAME)
    (total_num_transactions,) = connection.fetch_one(
        "SELECT COUNT(*) FROM expenzy_payout"
    )
    (processing_num_transactions,) = connection.fetch_one(
        "SELECT COUNT(*) FROM expenzy_payout WHERE state = 'processing'"
    )
    (max_update_count,) = connection.fetch_one(
        "SELECT max(state_update_count) FROM expenzy_payout WHERE state = 'processing'"
    )
    return jsonify({'total_num_transactions': total_num_transactions, 'processing_num_transactions': processing_num_transactions, 'max_update_count': max_update_count})


if __name__ == "__main__":
    app.run(debug=True, port=5001)
