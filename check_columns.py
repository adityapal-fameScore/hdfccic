import pandas as pd

df = pd.read_excel('ApplicantDetailsOfParentAnd_Child.xlsx', nrows=5)
cols = df.columns.tolist()

with open('all_columns.txt', 'w', encoding='utf-8') as f:
    for c in cols:
        f.write(c + '\n')

print("Columns related to 'RM':")
for c in cols:
    if 'rm' in c.lower():
        print(c)

print("\nColumns related to 'name':")
for c in cols:
    if 'name' in c.lower():
        print(c)
