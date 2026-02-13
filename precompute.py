#!/usr/bin/env python3
"""
PRECOMPUTE SCRIPT — Run this BEFORE starting the web server.
Loads Excel files, computes everything, and saves to JSON files.
The web server (sCRYPT2.py) will only read these JSON files.

Usage:  python precompute.py
"""

import pandas as pd
import numpy as np
import json
import os
import time
from collections import defaultdict

# ============================================================================
# CONFIG
# ============================================================================

SOURCE_FILES = [
    'BRE_Context_ChildvsParent.xlsx',
    'ApplicantDetailsOfParentAnd_Child.xlsx'
]

OUTPUT_DASHBOARD   = 'dashboard_records.json'
OUTPUT_PORTFOLIO   = 'portfolio_metrics.json'
OUTPUT_COMPARISON  = 'comparison_lookup.json'

# Columns needed for comparison lookup (per LOS ID)
COMPARISON_COLUMNS = [
    'pan_no', 'program_name', 'approval_status', 'created_on', 'SalesRMName',
    
    # Critical Parameters
    'CommercialBureaucmrScore', 'ConsumerBureaucreditScore',
    'ConsumerBureaunoOfWillFulDefaults', 'CommercialBureaunoOfWillFulDefaults',
    'ConsumerBureaunoOfWriteOffSuitFiledDoubtSubStd', 'CommercialBureaunoOfWriteOffSuitFiledDoubtSubStd',
    'ConsumerBureaunoOfNPA', 'CommercialBureaunoOfNPA',
    'credit_limit_amt',
    'ConsumerBureautotalWorkingCapitalSetup', 'CommercialBureautotalWorkingCapitalSetup',
    'BankDataSummarychequeReturnInwardCount',

    # Standard Parameters
    'ConsumerBureaufifteenPlusDPD', 'CommercialBureaufifteenPlusDPD',
    'ConsumerBureauthirtyPlusDPD', 'CommercialBureauthirtyPlusDPD',
    'BankDataSummarybtoOfAvgPurchaseLastTwelveMonths', 'BankDataSummarybtoOfAvgTurnOverLastTwelveMonths',
    'GstnDatalastTwelveMonthPurchase', 'GstnDatalastTwelveMonthTurnOver',
    'GstnDatagrossProfitPercentageInLastTwelveMonths',
    'GstnDatagstDelayCountGreaterThanOrEqualsTwentyDaysInLastTwelveMonths',
    'GstnDatatotalGst3BFilingDelayInLastTwelveMonths',
    'BankDataSummaryaveMonthlyCredit', 'BankDataSummaryaveMonthlyDebit',
    'GstnDataavePurchaseInLastTwelveMonthsGST2A', 'GstnDataaveSalesInLastTwelveMonthsGSTR1',
    'ConsumerBureautotalUSLOutStanding', 'CommercialBureautotalUSLOutStanding',
    'mcpBtoOfAvgTurnOverLastTwelveMonths', 'mcpCreditScore', 'mcpLastTwelveMonthTurnOver',

    # Deviations
    'BankDataSummaryDeviation', 'GstnDataDeviation', 'BusinessProfileDeviation',
    'PosidexDeviation', 'ConsumerBureauDeviation', 'CommercialBureauDeviation'
]


# ============================================================================
# STEP 1: LOAD & MERGE DATA
# ============================================================================

def load_and_merge():
    print("\n" + "=" * 80)
    print("STEP 1: LOADING & MERGING DATA")
    print("=" * 80)

    print("\n  Loading BRE_Context_ChildvsParent.xlsx...")
    df_bre = pd.read_excel(SOURCE_FILES[0])
    print(f"    ✓ {len(df_bre):,} records")

    print("  Loading ApplicantDetailsOfParentAnd_Child.xlsx...")
    df_applicant = pd.read_excel(SOURCE_FILES[1])
    print(f"    ✓ {len(df_applicant):,} records")

    print("  Merging datasets...")
    # Add SalesRMName to the selected columns from df_applicant
    df_merged = pd.merge(
        df_bre,
        df_applicant[['loan_request_id', 'nature_business', 'stateFC', 'stateApplicant', 'SalesRMName',
                      'BankDataSummaryDeviation', 'GstnDataDeviation', 'BusinessProfileDeviation',
                      'PosidexDeviation', 'ConsumerBureauDeviation', 'CommercialBureauDeviation']],
        on='loan_request_id',
        how='left'
    )

    df_merged['state'] = df_merged['stateFC'].fillna(df_merged['stateApplicant'])
    df_merged['industry'] = df_merged['nature_business'].fillna('Unknown')

    df_merged['created_on'] = pd.to_datetime(df_merged['created_on'], format='mixed', errors='coerce')
    df_merged['sanction_date'] = pd.to_datetime(df_merged['sanction_date'], format='mixed', errors='coerce')

    # Convert all metric columns to numeric
    numeric_cols = [
        'CommercialBureaucmrScore', 'ConsumerBureaucreditScore',
        'ConsumerBureaunoOfWillFulDefaults', 'CommercialBureaunoOfWillFulDefaults',
        'ConsumerBureaunoOfWriteOffSuitFiledDoubtSubStd', 'CommercialBureaunoOfWriteOffSuitFiledDoubtSubStd',
        'ConsumerBureaunoOfNPA', 'CommercialBureaunoOfNPA',
        'credit_limit_amt',
        'ConsumerBureautotalWorkingCapitalSetup', 'CommercialBureautotalWorkingCapitalSetup',
        'BankDataSummarychequeReturnInwardCount',
        'ConsumerBureaufifteenPlusDPD', 'CommercialBureaufifteenPlusDPD',
        'ConsumerBureauthirtyPlusDPD', 'CommercialBureauthirtyPlusDPD',
        'BankDataSummarybtoOfAvgPurchaseLastTwelveMonths', 'BankDataSummarybtoOfAvgTurnOverLastTwelveMonths',
        'GstnDatalastTwelveMonthPurchase', 'GstnDatalastTwelveMonthTurnOver',
        'GstnDatagrossProfitPercentageInLastTwelveMonths',
        'GstnDatagstDelayCountGreaterThanOrEqualsTwentyDaysInLastTwelveMonths',
        'GstnDatatotalGst3BFilingDelayInLastTwelveMonths',
        'BankDataSummaryaveMonthlyCredit', 'BankDataSummaryaveMonthlyDebit',
        'GstnDataavePurchaseInLastTwelveMonthsGST2A', 'GstnDataaveSalesInLastTwelveMonthsGSTR1',
        'ConsumerBureautotalUSLOutStanding', 'CommercialBureautotalUSLOutStanding',
        'mcpBtoOfAvgTurnOverLastTwelveMonths', 'mcpCreditScore', 'mcpLastTwelveMonthTurnOver'
    ]
    
    for col in numeric_cols:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')

    df_merged.set_index('loan_request_id', inplace=True)
    print(f"    ✓ Merged: {len(df_merged):,} records")
    return df_merged


# ============================================================================
# STEP 2: BUILD DASHBOARD RECORDS
# ============================================================================

def build_dashboard_records(df_merged):
    print("\n" + "=" * 80)
    print("STEP 2: BUILDING DASHBOARD RECORDS")
    print("=" * 80)

    pan_groups = df_merged.groupby('pan_no')
    dashboard_records = []

    for pan, group in pan_groups:
        group_sorted = group.sort_values('created_on')
        if len(group_sorted) >= 2:
            parent = group_sorted.iloc[0]
            child = group_sorted.iloc[-1]

            if parent.name != child.name:
                dashboard_records.append({
                    'pan': pan,
                    'old_los_id': int(parent.name),
                    'new_los_id': int(child.name),
                    'old_program': str(parent['program_name']),
                    'new_program': str(child['program_name']),
                    'old_status': str(parent['approval_status']),
                    'new_status': str(child['approval_status']),
                    'old_created': parent['created_on'].strftime('%Y-%m-%d') if pd.notna(parent['created_on']) else '—',
                    'new_created': child['created_on'].strftime('%Y-%m-%d') if pd.notna(child['created_on']) else '—',
                    'industry': str(child.get('industry', 'Unknown')),
                    'state': str(child.get('state', 'Unknown'))
                })

    print(f"    ✓ Built {len(dashboard_records):,} parent-child pairs")
    return dashboard_records


# ============================================================================
# STEP 3: CALCULATE 17 METRICS
# ============================================================================

def count_deviations(record):
    deviation_cols = ['BankDataSummaryDeviation', 'GstnDataDeviation', 'BusinessProfileDeviation',
                      'PosidexDeviation', 'ConsumerBureauDeviation', 'CommercialBureauDeviation']
    count = 0
    for col in deviation_cols:
        val = record.get(col)
        if pd.notna(val) and val not in [0, '0', 'No', 'NO', 'no', False, 'False']:
            # Split by ' || ' to count multiple deviations in one column
            segments = [s for s in str(val).split(' || ') if s.strip()]
            count += len(segments)
    return count


def calculate_all_17_metrics(df_merged, dashboard_records):
    print("\n" + "=" * 80)
    print("STEP 3: CALCULATING 17 PORTFOLIO METRICS")
    print("=" * 80)

    metrics = {
        'total_pans': len(dashboard_records),
        'cibil_drop_50plus': [], 'cmr_increase_2plus': [], 'cmr_rank_gt7_child': [],
        'cibil_lt700_child': [], 'willful_default_parent': [], 'willful_default_child': [],
        'dpd_15plus_child': [], 'dpd_15plus_improved': [], 'dpd_15plus_degraded': [],
        'dpd_30plus_child': [], 'dpd_30plus_improved': [], 'dpd_30plus_degraded': [],
        'sales_dropped': [], 'usl_increase_30pct': [], 'purchase_gt_sales_30pct': [],
        'turnover_not_increased': [], 'turnover_decreased': [], 'bto_drop_25pct': [],
        'bto_gt_150pct': [], 'deviation_count_1': [], 'deviation_count_2': [], 'deviation_count_3plus': [],
        'total_sales_change': 0, 'sales_count': 0
    }

    def make_dr_record(record, val_info=None):
        return {
            'pan': record['pan'], 'old_los': record['old_los_id'], 'new_los': record['new_los_id'],
            'industry': record['industry'], 'state': record['state'], 'info': val_info,
            'old_status': record['old_status'], 'new_status': record['new_status']
        }

    industry_metrics = defaultdict(lambda: {k: 0 for k in metrics if isinstance(metrics[k], list)})
    state_metrics = defaultdict(lambda: {k: 0 for k in metrics if isinstance(metrics[k], list)})

    total = len(dashboard_records)
    for idx, record in enumerate(dashboard_records):
        if idx % 500 == 0:
            print(f"  Processing {idx}/{total}...", end='\r')

        try:
            old_rec = df_merged.loc[record['old_los_id']]
            new_rec = df_merged.loc[record['new_los_id']]
            ind = record['industry']
            st = record['state']

            # 1. CIBIL Drop >= 50
            oc = old_rec.get('ConsumerBureaucreditScore')
            nc = new_rec.get('ConsumerBureaucreditScore')
            if pd.notna(oc) and pd.notna(nc) and oc > 0 and nc > 0 and (oc - nc) >= 50:
                metrics['cibil_drop_50plus'].append(make_dr_record(record, f"{oc}->{nc}"))
                industry_metrics[ind]['cibil_drop_50plus'] += 1
                state_metrics[st]['cibil_drop_50plus'] += 1

            # 2. CMR Increase >= 2
            ocm = old_rec.get('CommercialBureaucmrScore')
            ncm = new_rec.get('CommercialBureaucmrScore')
            if pd.notna(ocm) and pd.notna(ncm) and ocm > 0 and ncm > 0 and (ncm - ocm) >= 2 and ncm != 10:
                metrics['cmr_increase_2plus'].append(make_dr_record(record, f"{ocm}->{ncm}"))
                industry_metrics[ind]['cmr_increase_2plus'] += 1
                state_metrics[st]['cmr_increase_2plus'] += 1

            # 3. CMR > 7
            if pd.notna(ncm) and ncm > 7:
                metrics['cmr_rank_gt7_child'].append(make_dr_record(record, f"CMR: {ncm}"))
                industry_metrics[ind]['cmr_rank_gt7_child'] += 1
                state_metrics[st]['cmr_rank_gt7_child'] += 1

            # 4. CIBIL < 700
            if pd.notna(nc) and 0 < nc < 700:
                metrics['cibil_lt700_child'].append(make_dr_record(record, f"CIBIL: {nc}"))
                industry_metrics[ind]['cibil_lt700_child'] += 1
                state_metrics[st]['cibil_lt700_child'] += 1

            # 5. Willful Default
            wd_new = (new_rec.get('ConsumerBureaunoOfWillFulDefaults', 0) or 0) + (new_rec.get('CommercialBureaunoOfWillFulDefaults', 0) or 0)
            if wd_new > 0:
                metrics['willful_default_child'].append(make_dr_record(record, f"WD: {wd_new}"))
                industry_metrics[ind]['willful_default_child'] += 1
                state_metrics[st]['willful_default_child'] += 1

            # 6 & 7. DPD 15+
            dpd15_new = (new_rec.get('ConsumerBureaufifteenPlusDPD', 0) or 0) + (new_rec.get('CommercialBureaufifteenPlusDPD', 0) or 0)
            if dpd15_new > 0:
                metrics['dpd_15plus_child'].append(make_dr_record(record, f"DPD15+: {dpd15_new}"))
                industry_metrics[ind]['dpd_15plus_child'] += 1
                state_metrics[st]['dpd_15plus_child'] += 1

            # DPD 30+
            dpd30_new = (new_rec.get('ConsumerBureauthirtyPlusDPD', 0) or 0) + (new_rec.get('CommercialBureauthirtyPlusDPD', 0) or 0)
            if dpd30_new > 0:
                metrics['dpd_30plus_child'].append(make_dr_record(record, f"DPD30+: {dpd30_new}"))
                industry_metrics[ind]['dpd_30plus_child'] += 1
                state_metrics[st]['dpd_30plus_child'] += 1

            # 8. Sales Dropped
            osales = old_rec.get('GstnDatalastTwelveMonthTurnOver')
            nsales = new_rec.get('GstnDatalastTwelveMonthTurnOver')
            if pd.notna(osales) and pd.notna(nsales) and osales > 0:
                if nsales < (osales * 0.9):
                    metrics['sales_dropped'].append(make_dr_record(record, f"{osales:,.0f}->{nsales:,.0f}"))
                    industry_metrics[ind]['sales_dropped'] += 1
                    state_metrics[st]['sales_dropped'] += 1

                # 11 & 12 Turnover
                if nsales <= osales:
                    metrics['turnover_not_increased'].append(make_dr_record(record))
                    industry_metrics[ind]['turnover_not_increased'] += 1
                    state_metrics[st]['turnover_not_increased'] += 1
                if nsales < osales:
                    metrics['turnover_decreased'].append(make_dr_record(record))
                    industry_metrics[ind]['turnover_decreased'] += 1
                    state_metrics[st]['turnover_decreased'] += 1

            # 9. USL
            ousl = (old_rec.get('ConsumerBureautotalUSLOutStanding', 0) or 0) + (old_rec.get('CommercialBureautotalUSLOutStanding', 0) or 0)
            nusl = (new_rec.get('ConsumerBureautotalUSLOutStanding', 0) or 0) + (new_rec.get('CommercialBureautotalUSLOutStanding', 0) or 0)
            if ousl > 0 and ((nusl - ousl) / ousl) >= 0.3:
                metrics['usl_increase_30pct'].append(make_dr_record(record, f"{ousl:,.0f}->{nusl:,.0f}"))
                industry_metrics[ind]['usl_increase_30pct'] += 1
                state_metrics[st]['usl_increase_30pct'] += 1

            # 10. Purchase > Sales
            npurch = new_rec.get('GstnDatalastTwelveMonthPurchase')
            if pd.notna(npurch) and pd.notna(nsales) and nsales > 0 and ((npurch - nsales) / nsales) >= 0.3:
                metrics['purchase_gt_sales_30pct'].append(make_dr_record(record, f"P:{npurch:,.0f} > S:{nsales:,.0f}"))
                industry_metrics[ind]['purchase_gt_sales_30pct'] += 1
                state_metrics[st]['purchase_gt_sales_30pct'] += 1

            # 13 & 14. BTO
            obto = old_rec.get('BankDataSummarybtoOfAvgTurnOverLastTwelveMonths')
            nbto = new_rec.get('BankDataSummarybtoOfAvgTurnOverLastTwelveMonths')
            if pd.notna(obto) and pd.notna(nbto) and obto > 0 and ((nbto - obto) / obto) <= -0.25:
                metrics['bto_drop_25pct'].append(make_dr_record(record, f"{obto}%->{nbto}%"))
                industry_metrics[ind]['bto_drop_25pct'] += 1
                state_metrics[st]['bto_drop_25pct'] += 1
            if pd.notna(nbto) and nbto > 150:
                metrics['bto_gt_150pct'].append(make_dr_record(record, f"{nbto}%"))
                industry_metrics[ind]['bto_gt_150pct'] += 1
                state_metrics[st]['bto_gt_150pct'] += 1

            # 15, 16, 17. Deviations
            devs = count_deviations(new_rec)
            if devs == 1:
                metrics['deviation_count_1'].append(make_dr_record(record, "1 Dev"))
                industry_metrics[ind]['deviation_count_1'] += 1
                state_metrics[st]['deviation_count_1'] += 1
            elif devs == 2:
                metrics['deviation_count_2'].append(make_dr_record(record, "2 Devs"))
                industry_metrics[ind]['deviation_count_2'] += 1
                state_metrics[st]['deviation_count_2'] += 1
            elif devs >= 3:
                metrics['deviation_count_3plus'].append(make_dr_record(record, f"{devs} Devs"))
                industry_metrics[ind]['deviation_count_3plus'] += 1
                state_metrics[st]['deviation_count_3plus'] += 1

        except Exception:
            continue

    # Flatten counts
    metrics['counts'] = {k: len(v) for k, v in metrics.items() if isinstance(v, list)}

    # Breakdowns
    metrics['industry_breakdown'] = sorted(
        [{'name': k, **v} for k, v in industry_metrics.items()],
        key=lambda x: x.get('cibil_drop_50plus', 0), reverse=True
    )
    metrics['state_breakdown'] = sorted(
        [{'name': k, **v} for k, v in state_metrics.items()],
        key=lambda x: x.get('cibil_drop_50plus', 0), reverse=True
    )

    print(f"\n    ✓ Metrics calculated for {total:,} records")
    return metrics


# ============================================================================
# STEP 4: BUILD COMPARISON LOOKUP
# ============================================================================

def build_comparison_lookup(df_merged):
    """Build a dict keyed by loan_request_id with only the columns needed for comparison."""
    print("\n" + "=" * 80)
    print("STEP 4: BUILDING COMPARISON LOOKUP")
    print("=" * 80)

    lookup = {}
    for los_id, row in df_merged.iterrows():
        entry = {}
        for col in COMPARISON_COLUMNS:
            val = row.get(col)
            if pd.isna(val):
                entry[col] = None
            elif isinstance(val, pd.Timestamp):
                entry[col] = val.strftime('%Y-%m-%d')
            elif isinstance(val, (np.integer,)):
                entry[col] = int(val)
            elif isinstance(val, (np.floating,)):
                entry[col] = float(val)
            else:
                entry[col] = str(val)
        lookup[str(los_id)] = entry

    print(f"    ✓ Built lookup for {len(lookup):,} LOS IDs")
    return lookup


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    start = time.time()

    print("\n" + "█" * 80)
    print("  PRECOMPUTE: Generating all data files for the web server")
    print("█" * 80)

    # Check source files exist
    for f in SOURCE_FILES:
        if not os.path.exists(f):
            print(f"\n  ✗ ERROR: Source file not found: {f}")
            print("    Place the Excel files in the same directory as this script.")
            exit(1)

    # Step 1: Load & merge
    df_merged = load_and_merge()

    # Step 2: Dashboard records
    dashboard_records = build_dashboard_records(df_merged)

    # Step 3: 17 metrics
    portfolio_metrics = calculate_all_17_metrics(df_merged, dashboard_records)

    # Step 4: Comparison lookup
    comparison_lookup = build_comparison_lookup(df_merged)

    # Save all outputs
    print("\n" + "=" * 80)
    print("SAVING OUTPUT FILES")
    print("=" * 80)

    with open(OUTPUT_DASHBOARD, 'w', encoding='utf-8') as f:
        json.dump(dashboard_records, f, ensure_ascii=False)
    print(f"  ✓ {OUTPUT_DASHBOARD} ({len(dashboard_records):,} records)")

    with open(OUTPUT_PORTFOLIO, 'w', encoding='utf-8') as f:
        json.dump(portfolio_metrics, f, ensure_ascii=False)
    print(f"  ✓ {OUTPUT_PORTFOLIO}")

    with open(OUTPUT_COMPARISON, 'w', encoding='utf-8') as f:
        json.dump(comparison_lookup, f, ensure_ascii=False)
    print(f"  ✓ {OUTPUT_COMPARISON} ({len(comparison_lookup):,} LOS IDs)")

    elapsed = time.time() - start
    print(f"\n{'█' * 80}")
    print(f"  ✓ DONE in {elapsed:.1f}s — All files ready. Now run: python sCRYPT2.py")
    print(f"{'█' * 80}\n")
