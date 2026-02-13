import json
from collections import Counter

with open('dashboard_records.json', 'r') as f:
    data = json.load(f)

print("Unique New Programs:")
for k, v in Counter(r['new_program'] for r in data).items():
    print(f"'{k}': {v}")
