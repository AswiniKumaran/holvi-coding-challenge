"""
Quick & dirty script to check the state on both services
"""

import requests

holvi_payout_count = int(requests.get("http://holvi-api:5002/payout/count").content)
expenzy_transaction_counts = requests.get("http://expenzy-server:5001/api/transaction/count").json()
expenzy_transaction_total_count = expenzy_transaction_counts["total_num_transactions"]
expenzy_transaction_processing_count = expenzy_transaction_counts["processing_num_transactions"]
expenzy_max_update_count = expenzy_transaction_counts["max_update_count"]

print("Number of records @ Holvi:  ", holvi_payout_count)
print("Number of records @ Expenzy:", expenzy_transaction_total_count)
print("Number of processing records @ Expenzy:", expenzy_transaction_processing_count)
print("Max amount of updates to single record @ Expenzy:", expenzy_max_update_count)

processing_difference = abs(holvi_payout_count - expenzy_transaction_processing_count)
print("Processing diff:", processing_difference, ":)" if processing_difference == 0 else ":(")
total_difference = abs(holvi_payout_count - expenzy_transaction_total_count)
print("Total diff:", total_difference, ":)" if total_difference == 0 else ":(")
