import pandas as pd
import os

FILE = 'ApplicantDetailsOfParentAnd_Child.xlsx'
df = pd.read_excel(FILE)

deviation_cols = ['BankDataSummaryDeviation', 'GstnDataDeviation', 'BusinessProfileDeviation',
                  'PosidexDeviation', 'ConsumerBureauDeviation', 'CommercialBureauDeviation']

with open('deviation_report.txt', 'w', encoding='utf-8') as f:
    f.write("DEVIATION REPORT\n================\n")
    
    for col in deviation_cols:
        if col in df.columns:
            f.write(f"\n--- {col} ---\n")
            vc = df[col].astype(str).value_counts(dropna=False).head(20)
            f.write(vc.to_string())
            f.write("\n")

    f.write("\n\nANALYSIS OF COUNTS PER ROW\n")
    
    def get_active_devs(row):
        active = []
        for col in deviation_cols:
            val = row.get(col)
            if pd.notna(val) and val not in [0, '0', 'No', 'NO', 'no', False, 'False']:
                active.append(f"{col}={val}")
        return active

    df['active_devs'] = df.apply(get_active_devs, axis=1)
    df['dev_count'] = df['active_devs'].apply(len)
    
    counts = df['dev_count'].value_counts().sort_index()
    f.write(f"\nDistribution of Deviation Counts per Row:\n{counts.to_string()}\n")
    
    f.write("\nSample Rows with 1 Deviation:\n")
    sample = df[df['dev_count'] == 1].head(5)
    for idx, row in sample.iterrows():
        f.write(f"ID {row.get('loan_request_id')}: {row['active_devs']}\n")

    f.write("\nSample Rows with >1 Deviation (if any):\n")
    sample_multi = df[df['dev_count'] > 1].head(5)
    if len(sample_multi) == 0:
        f.write("NONE FOUND\n")
    else:
        for idx, row in sample_multi.iterrows():
            f.write(f"ID {row.get('loan_request_id')}: {row['active_devs']}\n")
            
print("Report generated: deviation_report.txt")
