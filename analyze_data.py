import json
from collections import Counter

with open('dashboard_records.json', 'r') as f:
    data = json.load(f)

print(f"Total records: {len(data)}")

programs = Counter(r.get('new_program') for r in data)
print("\nProgram Distribution:")
for p, count in programs.items():
    print(f"  '{p}': {count}")

print(f"\nSum (Renewal + Enhancement): {programs['Renewal program'] + programs['Enhancement program']}")

# Check for extra spaces or case sensitivity
print("\nDetailed Check (All Unique Programs):")
for p in set(r.get('new_program', '') for r in data):
    print(f"  '{p}' (repr: {repr(p)})")
