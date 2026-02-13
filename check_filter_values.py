import json
from collections import Counter

with open('dashboard_records.json', 'r') as f:
    data = json.load(f)

print("Unique New Statuses:")
print(Counter(r['new_status'] for r in data))

print("\nUnique New Programs:")
print(Counter(r['new_program'] for r in data))
