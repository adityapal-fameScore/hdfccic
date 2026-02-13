import json

with open('portfolio_metrics.json', 'r') as f:
    data = json.load(f)

print(f"Total PANs: {data.get('total_pans')}")
print(f"1 Deviation: {len(data.get('deviation_count_1', []))} records")
print(f"2 Deviations: {len(data.get('deviation_count_2', []))} records")
print(f"3+ Deviations: {len(data.get('deviation_count_3plus', []))} records")

if data.get('deviation_count_2'):
    print("\nSample record with 2 deviations:")
    print(data['deviation_count_2'][0])
