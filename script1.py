#!/usr/bin/env python3
"""
LOS Comparison Dashboard - Single Page Application
Enhanced with Portfolio Analysis Page and CSV Caching
"""

from flask import Flask, jsonify, request
import pandas as pd
import numpy as np
from datetime import datetime
from functools import lru_cache
import json
import os

app = Flask(__name__)

# ============================================================================
# GLOBAL VARIABLES & CACHING
# ============================================================================

df = None
dashboard_cache = None
portfolio_analysis_cache = None
available_columns = set()
CACHE_VERSION = 0
PORTFOLIO_CACHE_FILE = 'portfolio_analysis_cache.csv'
DASHBOARD_CACHE_FILE = 'dashboard_cache.json'


# ============================================================================
# CSV CACHE FUNCTIONS
# ============================================================================

def save_portfolio_cache(analysis_data):
    """Save portfolio analysis to CSV files for faster loading"""
    print("Saving portfolio analysis to cache files...")

    try:
        # Save each analysis type to a separate CSV
        for analysis_type in ['overall', 'approved', 'rejected']:
            # Create a copy without the case arrays (too large for CSV)
            cache_data = []
            for param in analysis_data[analysis_type]:
                param_copy = param.copy()
                # Remove case arrays - they'll be computed on demand
                param_copy.pop('improved_cases', None)
                param_copy.pop('degraded_cases', None)
                param_copy.pop('unchanged_cases', None)
                param_copy.pop('no_data_cases', None)
                cache_data.append(param_copy)

            df_cache = pd.DataFrame(cache_data)
            cache_file = f'portfolio_cache_{analysis_type}.csv'
            df_cache.to_csv(cache_file, index=False)
            print(f"  ✓ Saved {cache_file}")

        # Create a metadata file to track when cache was created
        metadata = {
            'created_at': datetime.now().isoformat(),
            'num_parameters': len(analysis_data['overall'])
        }
        with open('portfolio_cache_metadata.json', 'w') as f:
            json.dump(metadata, f)

        print("✓ Portfolio cache saved successfully")
        return True
    except Exception as e:
        print(f"✗ Error saving portfolio cache: {e}")
        return False


def load_portfolio_cache():
    """Load portfolio analysis from CSV files if they exist"""
    print("Checking for cached portfolio analysis...")

    try:
        # Check if all required files exist
        required_files = [
            'portfolio_cache_overall.csv',
            'portfolio_cache_approved.csv',
            'portfolio_cache_rejected.csv',
            'portfolio_cache_metadata.json'
        ]

        if not all(os.path.exists(f) for f in required_files):
            print("  Cache files not found")
            return None

        # Load metadata
        with open('portfolio_cache_metadata.json', 'r') as f:
            metadata = json.load(f)

        print(f"  Found cache created at: {metadata['created_at']}")

        # Load each analysis type
        analysis_data = {}
        for analysis_type in ['overall', 'approved', 'rejected']:
            cache_file = f'portfolio_cache_{analysis_type}.csv'
            df_cache = pd.read_csv(cache_file)

            # Convert to dict and add empty case arrays (will be computed on demand)
            records = df_cache.to_dict('records')
            for record in records:
                record['improved_cases'] = []
                record['degraded_cases'] = []
                record['unchanged_cases'] = []
                record['no_data_cases'] = []

            analysis_data[analysis_type] = records

        print(f"✓ Loaded portfolio cache with {metadata['num_parameters']} parameters")
        print("  Note: Case arrays will be computed on-demand for drill-down")
        return analysis_data

    except Exception as e:
        print(f"✗ Error loading portfolio cache: {e}")
        return None


def save_dashboard_cache(dashboard_records):
    """Save dashboard records to JSON for faster loading"""
    print("Saving dashboard cache...")

    try:
        with open(DASHBOARD_CACHE_FILE, 'w') as f:
            json.dump(dashboard_records, f)
        print(f"✓ Saved {len(dashboard_records)} dashboard records to cache")
        return True
    except Exception as e:
        print(f"✗ Error saving dashboard cache: {e}")
        return False


def load_dashboard_cache():
    """Load dashboard records from JSON if it exists"""
    print("Checking for cached dashboard data...")

    try:
        if not os.path.exists(DASHBOARD_CACHE_FILE):
            print("  Dashboard cache file not found")
            return None

        with open(DASHBOARD_CACHE_FILE, 'r') as f:
            dashboard_records = json.load(f)

        print(f"✓ Loaded {len(dashboard_records)} dashboard records from cache")
        return dashboard_records

    except Exception as e:
        print(f"✗ Error loading dashboard cache: {e}")
        return None


def clear_all_caches():
    """Delete all cache files"""
    cache_files = [
        'portfolio_cache_overall.csv',
        'portfolio_cache_approved.csv',
        'portfolio_cache_rejected.csv',
        'portfolio_cache_metadata.json',
        DASHBOARD_CACHE_FILE
    ]

    for cache_file in cache_files:
        if os.path.exists(cache_file):
            os.remove(cache_file)
            print(f"Deleted {cache_file}")


# ============================================================================
# DATA LOADING AND PROCESSING (OPTIMIZED)
# ============================================================================

def load_data():
    """Load and clean the Excel data with optimizations"""
    print("Loading Excel file...")
    df = pd.read_excel('BRE_Context_ChildvsParent.xlsx')

    # Store available columns globally
    global available_columns
    available_columns = set(df.columns)
    print(f"Available columns: {len(available_columns)}")

    print("Processing datetime columns...")
    # Use faster datetime conversion
    df['created_on'] = pd.to_datetime(df['created_on'], format='mixed', errors='coerce')
    df['sanction_date'] = pd.to_datetime(df['sanction_date'], format='mixed', errors='coerce')

    print("Converting numeric columns...")
    # Convert string numeric columns in bulk - only if they exist
    numeric_cols = ['GstnDatalastTwelveMonthTurnOver', 'mcpLastTwelveMonthTurnOver']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Pre-convert loan_request_id to int for faster lookups
    df['loan_request_id'] = df['loan_request_id'].astype(int)

    # Create index for faster lookups
    print("Creating indexes...")
    df.set_index('loan_request_id', inplace=True, drop=False)

    return df


def precompute_dashboard_data(df):
    """Pre-compute all dashboard records once at startup"""
    print("Pre-computing dashboard records...")
    dashboard_records = []

    # Group by PAN and process in vectorized manner
    grouped = df.groupby('pan_no')

    for pan, group in grouped:
        # Sort once
        group_sorted = group.sort_values('created_on')

        # Filter programs
        old_mask = group_sorted['program_name'] == 'Od program'
        new_mask = (group_sorted['program_name'] == 'Renewal program') | \
                   (group_sorted['program_name'] == 'Enhancement program')

        old_programs = group_sorted[old_mask]
        new_programs = group_sorted[new_mask]

        if len(old_programs) > 0 and len(new_programs) > 0:
            old_los = old_programs.iloc[0]
            new_los = new_programs.iloc[-1]

            record = {
                'pan': pan,
                'old_los_id': int(old_los['loan_request_id']),
                'old_program': old_los['program_name'],
                'old_status': old_los['approval_status'],
                'old_created': old_los['created_on'].strftime('%Y-%m-%d'),
                'new_los_id': int(new_los['loan_request_id']),
                'new_program': new_los['program_name'],
                'new_status': new_los['approval_status'],
                'new_created': new_los['created_on'].strftime('%Y-%m-%d'),
                # Pre-compute search strings for faster filtering
                'search_string': f"{pan.upper()}|{old_los['loan_request_id']}|{new_los['loan_request_id']}"
            }

            dashboard_records.append(record)

    print(f"Pre-computed {len(dashboard_records)} dashboard records")
    return dashboard_records


def get_dashboard_data(search_query=''):
    """Get dashboard records with optional search (uses cache)"""
    global dashboard_cache

    if not search_query:
        return dashboard_cache

    # Fast search using pre-computed search strings
    search_upper = search_query.upper()
    filtered = [
        record for record in dashboard_cache
        if search_upper in record['search_string']
    ]

    return filtered


def get_available_comparison_params():
    """Get list of comparison parameters that exist in the dataset"""
    global available_columns

    # Define all parameters with (name, column, higher_is_better, is_critical)
    all_params = [
        # CRITICAL PARAMETERS - Listed First
        ('Commercial CMR Score', 'CommercialBureaucmrScore', False, True),
        ('Consumer Credit Score', 'ConsumerBureaucreditScore', True, True),
        ('Consumer Wilful Defaults', 'ConsumerBureaunoOfWillFulDefaults', False, True),
        ('Commercial Wilful Defaults', 'CommercialBureaunoOfWillFulDefaults', False, True),
        ('Consumer Write-offs', 'ConsumerBureaunoOfWriteOffSuitFiledDoubtSubStd', False, True),
        ('Commercial Write-offs', 'CommercialBureaunoOfWriteOffSuitFiledDoubtSubStd', False, True),
        ('Consumer NPA', 'ConsumerBureaunoOfNPA', False, True),
        ('Commercial NPA', 'CommercialBureaunoOfNPA', False, True),
        ('Credit Limit', 'credit_limit_amt', True, True),
        ('Consumer Working Capital', 'ConsumerBureautotalWorkingCapitalSetup', True, True),
        ('Commercial Working Capital', 'CommercialBureautotalWorkingCapitalSetup', True, True),
        ('Cheque Return Count', 'BankDataSummarychequeReturnInwardCount', False, True),

        # STANDARD PARAMETERS
        ('Consumer 15+ DPD', 'ConsumerBureaufifteenPlusDPD', False, False),
        ('Commercial 15+ DPD', 'CommercialBureaufifteenPlusDPD', False, False),
        ('Consumer 30+ DPD', 'ConsumerBureauthirtyPlusDPD', False, False),
        ('Commercial 30+ DPD', 'CommercialBureauthirtyPlusDPD', False, False),
        ('Bank BTO Avg Purchase', 'BankDataSummarybtoOfAvgPurchaseLastTwelveMonths', True, False),
        ('Bank BTO Avg Turnover', 'BankDataSummarybtoOfAvgTurnOverLastTwelveMonths', True, False),
        ('GSTN Purchase (12M)', 'GstnDatalastTwelveMonthPurchase', True, False),
        ('GSTN Turnover (12M)', 'GstnDatalastTwelveMonthTurnOver', True, False),
        ('GSTN Gross Profit %', 'GstnDatagrossProfitPercentageInLastTwelveMonths', True, False),
        ('GST Delay Count (≥20d)', 'GstnDatagstDelayCountGreaterThanOrEqualsTwentyDaysInLastTwelveMonths', False,
         False),
        ('GST 3B Filing Delay', 'GstnDatatotalGst3BFilingDelayInLastTwelveMonths', False, False),
        ('Bank Avg Monthly Credit', 'BankDataSummaryaveMonthlyCredit', True, False),
        ('Bank Avg Monthly Debit', 'BankDataSummaryaveMonthlyDebit', False, False),
        ('GSTN Avg Purchase (GST2A)', 'GstnDataavePurchaseInLastTwelveMonthsGST2A', True, False),
        ('GSTN Avg Sales (GSTR1)', 'GstnDataaveSalesInLastTwelveMonthsGSTR1', True, False),
        ('Consumer USL Outstanding', 'ConsumerBureautotalUSLOutStanding', False, False),
        ('Commercial USL Outstanding', 'CommercialBureautotalUSLOutStanding', False, False),
        ('MCP BTO Avg Turnover', 'mcpBtoOfAvgTurnOverLastTwelveMonths', True, False),
        ('MCP Credit Score', 'mcpCreditScore', True, False),
        ('MCP Turnover (12M)', 'mcpLastTwelveMonthTurnOver', True, False),
    ]

    # Filter to only include columns that actually exist
    # Returns: (name, column, higher_is_better, is_critical)
    available_params = [
        (name, col, better, critical) for name, col, better, critical in all_params
        if col in available_columns
    ]

    # Sort: Critical parameters first, then standard parameters
    available_params.sort(key=lambda x: (not x[3], x[0]))

    return available_params


@lru_cache(maxsize=128)
def get_comparison_data_cached(old_los_id, new_los_id):
    """Generate detailed comparison between two LOS IDs (cached)"""
    return get_comparison_data(df, old_los_id, new_los_id)


def get_comparison_data(df, old_los_id, new_los_id):
    """Generate detailed comparison between two LOS IDs"""
    # Use index for O(1) lookup instead of O(n)
    old_los_id = int(old_los_id)
    new_los_id = int(new_los_id)

    old_record = df.loc[old_los_id]
    new_record = df.loc[new_los_id]

    # Get only available parameters
    comparison_params = get_available_comparison_params()

    comparisons = []
    has_critical_case = False

    for param_name, column_name, higher_is_better, is_critical in comparison_params:
        # Safe column access
        try:
            old_value = old_record[column_name] if column_name in old_record.index else None
            new_value = new_record[column_name] if column_name in new_record.index else None
        except:
            old_value = None
            new_value = None

        old_display = format_value(old_value)
        new_display = format_value(new_value)
        change_info = calculate_change(old_value, new_value, higher_is_better, param_name)

        # Detect highly critical cases
        is_highly_critical = False
        if old_value is not None and new_value is not None and not pd.isna(old_value) and not pd.isna(new_value):
            try:
                old_val = float(old_value)
                new_val = float(new_value)

                # CIBIL drop of 50+ points
                if 'Consumer Credit Score' in param_name or 'CIBIL' in param_name.upper():
                    if old_val - new_val >= 50:
                        is_highly_critical = True
                        has_critical_case = True

                # CMR increase of 2+ points (higher CMR is worse)
                elif 'Commercial CMR Score' in param_name or 'CMR' in param_name.upper():
                    if (new_val - old_val >= 2 and new_value !=2) and ( new_val - old_val > 0):
                        is_highly_critical = True
                        has_critical_case = True
            except:
                pass

        comparisons.append({
            'parameter': param_name,
            'old_value': old_display,
            'new_value': new_display,
            'change': change_info['change'],
            'change_percent': change_info['change_percent'],
            'status': change_info['status'],
            'is_critical': is_critical,
            'is_highly_critical': is_highly_critical
        })

    return {
        'pan': old_record['pan_no'],
        'old_los_id': old_los_id,
        'new_los_id': new_los_id,
        'old_program': old_record['program_name'],
        'new_program': new_record['program_name'],
        'old_status': old_record['approval_status'],
        'new_status': new_record['approval_status'],
        'old_created': old_record['created_on'].strftime('%Y-%m-%d') if pd.notna(old_record['created_on']) else '—',
        'new_created': new_record['created_on'].strftime('%Y-%m-%d') if pd.notna(new_record['created_on']) else '—',
        'comparisons': comparisons,
        'has_critical_case': has_critical_case
    }


def get_portfolio_analysis():
    """Generate portfolio-wide analysis for all parameters (uses cache)"""
    global portfolio_analysis_cache
    return portfolio_analysis_cache


def precompute_portfolio_analysis(dashboard_records):
    """Pre-compute portfolio analysis at startup"""
    print("Pre-computing portfolio analysis...")

    comparison_params = get_available_comparison_params()

    # Initialize results structure
    analysis_results = {
        'overall': [],
        'approved': [],
        'rejected': []
    }

    # Process each parameter
    for param_name, column_name, higher_is_better, is_critical in comparison_params:
        # Overall analysis
        overall_stats = analyze_parameter_across_portfolio(
            dashboard_records, column_name, param_name, higher_is_better, is_critical, filter_status=None
        )
        analysis_results['overall'].append(overall_stats)

        # Approved cases analysis
        approved_stats = analyze_parameter_across_portfolio(
            dashboard_records, column_name, param_name, higher_is_better, is_critical, filter_status='approved'
        )
        analysis_results['approved'].append(approved_stats)

        # Rejected cases analysis
        rejected_stats = analyze_parameter_across_portfolio(
            dashboard_records, column_name, param_name, higher_is_better, is_critical, filter_status='rejected'
        )
        analysis_results['rejected'].append(rejected_stats)

    print(f"✓ Portfolio analysis complete: {len(comparison_params)} parameters analyzed")
    return analysis_results


def analyze_parameter_across_portfolio(dashboard_records, column_name, param_name, higher_is_better, is_critical,
                                       filter_status=None):
    """Analyze a single parameter across all portfolio cases"""

    changes = []
    improved_count = 0
    degraded_count = 0
    unchanged_count = 0
    no_data_count = 0

    # Track case IDs for drill-down
    improved_cases = []
    degraded_cases = []
    unchanged_cases = []
    no_data_cases = []

    for record in dashboard_records:
        old_los_id = record['old_los_id']
        new_los_id = record['new_los_id']
        new_status = record['new_status'].lower()
        pan = record['pan']

        # Apply status filter if specified
        if filter_status:
            if filter_status == 'approved' and 'approved' not in new_status:
                continue
            elif filter_status == 'rejected' and 'rejected' not in new_status:
                continue

        try:
            old_record = df.loc[old_los_id]
            new_record = df.loc[new_los_id]

            old_value = old_record[column_name] if column_name in old_record.index else None
            new_value = new_record[column_name] if column_name in new_record.index else None

            if old_value is not None and new_value is not None and \
                    not pd.isna(old_value) and not pd.isna(new_value):
                try:
                    old_val = float(old_value)
                    new_val = float(new_value)
                    change = new_val - old_val
                    change_percent = (change / abs(old_val)) * 100 if old_val != 0 else 0

                    changes.append({
                        'absolute': change,
                        'percent': change_percent,
                        'old': old_val,
                        'new': new_val
                    })

                    # Categorize change
                    change_info = calculate_change(old_value, new_value, higher_is_better, param_name)
                    case_info = {'pan': pan, 'old_los_id': old_los_id, 'new_los_id': new_los_id}

                    if change_info['status'] == 'improved':
                        improved_count += 1
                        improved_cases.append(case_info)
                    elif change_info['status'] == 'degraded':
                        degraded_count += 1
                        degraded_cases.append(case_info)
                    elif change_info['status'] == 'unchanged':
                        unchanged_count += 1
                        unchanged_cases.append(case_info)
                    else:
                        no_data_count += 1
                        no_data_cases.append(case_info)

                except:
                    no_data_count += 1
                    no_data_cases.append({'pan': pan, 'old_los_id': old_los_id, 'new_los_id': new_los_id})
            else:
                no_data_count += 1
                no_data_cases.append({'pan': pan, 'old_los_id': old_los_id, 'new_los_id': new_los_id})
        except:
            no_data_count += 1
            no_data_cases.append({'pan': record['pan'], 'old_los_id': old_los_id, 'new_los_id': new_los_id})

    # Calculate statistics
    total_cases = improved_count + degraded_count + unchanged_count + no_data_count

    if changes:
        avg_change = np.mean([c['absolute'] for c in changes])
        avg_percent = np.mean([c['percent'] for c in changes])
        median_change = np.median([c['absolute'] for c in changes])
        std_change = np.std([c['absolute'] for c in changes])
        avg_old = np.mean([c['old'] for c in changes])
        avg_new = np.mean([c['new'] for c in changes])
    else:
        avg_change = 0
        avg_percent = 0
        median_change = 0
        std_change = 0
        avg_old = 0
        avg_new = 0

    return {
        'parameter': param_name,
        'column_name': column_name,
        'is_critical': is_critical,
        'total_cases': total_cases,
        'improved': improved_count,
        'degraded': degraded_count,
        'unchanged': unchanged_count,
        'no_data': no_data_count,
        'avg_change': round(avg_change, 2),
        'avg_percent_change': round(avg_percent, 2),
        'median_change': round(median_change, 2),
        'std_change': round(std_change, 2),
        'avg_old_value': round(avg_old, 2),
        'avg_new_value': round(avg_new, 2),
        'improvement_rate': round((improved_count / total_cases * 100), 1) if total_cases > 0 else 0,
        'degradation_rate': round((degraded_count / total_cases * 100), 1) if total_cases > 0 else 0,
        # Case IDs for drill-down
        'improved_cases': improved_cases,
        'degraded_cases': degraded_cases,
        'unchanged_cases': unchanged_cases,
        'no_data_cases': no_data_cases
    }


def format_value(value):
    """Format value for display"""
    if value is None or pd.isna(value):
        return '—'
    elif isinstance(value, (int, np.integer)):
        return f'{int(value):,}'
    elif isinstance(value, (float, np.floating)):
        return f'{float(value):,.2f}'
    return str(value)


def calculate_change(old_value, new_value, higher_is_better, parameter_name=''):
    """Calculate change between old and new values with special rules for CIBIL and CRM scores"""
    if old_value is None or new_value is None or pd.isna(old_value) or pd.isna(new_value):
        return {'change': '—', 'change_percent': '—', 'status': 'no-data'}

    try:
        old_val = float(old_value)
        new_val = float(new_value)
    except:
        return {'change': '—', 'change_percent': '—', 'status': 'no-data'}

    change = new_val - old_val
    change_percent = (change / abs(old_val)) * 100 if old_val != 0 else 0

    # Special logic for CIBIL Score (Consumer Credit Score)
    if 'Consumer Credit Score' in parameter_name or 'CIBIL' in parameter_name.upper():
        # If old score was -1 and new score >= 720, it's improved
        if old_val == -1 and new_val >= 720:
            status = 'improved'
        # If old score was -1 and new score is between 1-650, it's degraded
        elif old_val == -1 and 1 <= new_val <= 650:
            status = 'degraded'
        # If old score was -1 and new score is between 651-719, it's improved (better than nothing)
        elif old_val == -1 and 651 <= new_val < 720:
            status = 'improved'
        # Standard logic for other cases
        elif abs(change) < 0.01:
            status = 'unchanged'
        elif change > 0:  # Higher CIBIL is always better
            status = 'improved'
        else:
            status = 'degraded'

    # Special logic for CMR Score (Commercial CMR Score)
    elif 'Commercial CMR Score' in parameter_name or 'CMR' in parameter_name.upper():
        # Check if old value was NA (represented as -1 or very low number)
        old_is_na = old_val <= 0 or old_val == -1
        new_is_na = new_val <= 0 or new_val == -1

        # If old was NA and new score is 1-6, it's improved
        if old_is_na and 1 <= new_val <= 6:
            status = 'improved'
        # If old was valid (1-6) and new is >6 or NA, it's degraded
        elif (1 <= old_val <= 6) and (new_val > 6 or new_is_na):
            status = 'degraded'
        # If old was NA and new is >6 or still NA, it's degraded or unchanged
        elif old_is_na and (new_val > 6 or new_is_na):
            if new_is_na:
                status = 'unchanged'
            else:
                status = 'degraded'
        # Standard logic for scores within 1-6 range
        elif abs(change) < 0.01:
            status = 'unchanged'
        elif change < 0:  # Lower CMR score (1-6) is better
            status = 'improved'
        else:
            status = 'degraded'

    # Standard logic for all other parameters
    else:
        if abs(change) < 0.01:
            status = 'unchanged'
        elif (change > 0 and higher_is_better) or (change < 0 and not higher_is_better):
            status = 'improved'
        else:
            status = 'degraded'

    return {
        'change': f'{change:+,.2f}',
        'change_percent': f'{change_percent:+.1f}%',
        'status': status
    }


# ============================================================================
# INITIALIZATION WITH CACHE SUPPORT
# ============================================================================

print("=" * 60)
print("Initializing LOS Comparison Dashboard with Cache Support...")
print("=" * 60)

# Load data
df = load_data()
print(f"✓ Data loaded: {len(df)} records")
print(f"✓ Unique PANs: {df['pan_no'].nunique()}")

# Try to load dashboard cache, compute if not available
dashboard_cache = load_dashboard_cache()
if dashboard_cache is None:
    dashboard_cache = precompute_dashboard_data(df)
    save_dashboard_cache(dashboard_cache)
print(f"✓ Dashboard cache ready: {len(dashboard_cache)} records")

# Try to load portfolio cache, compute if not available
portfolio_analysis_cache = load_portfolio_cache()
if portfolio_analysis_cache is None:
    portfolio_analysis_cache = precompute_portfolio_analysis(dashboard_cache)
    save_portfolio_cache(portfolio_analysis_cache)
else:
    print("✓ Using cached portfolio analysis (instant load!)")

# Get and display available comparison parameters
available_params = get_available_comparison_params()
print(f"✓ Available comparison parameters: {len(available_params)}")
print("=" * 60)
print("💡 TIP: Delete cache files to force recomputation on next run")
print("=" * 60)


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    """Serve the single page application"""
    return HTML_TEMPLATE


@app.route('/api/dashboard')
def api_dashboard():
    """API endpoint for dashboard data"""
    search = request.args.get('search', '')
    data = get_dashboard_data(search)
    return jsonify({'data': data, 'total': len(dashboard_cache)})


@app.route('/api/compare')
def api_compare():
    """API endpoint for comparison data"""
    old_los_id = request.args.get('old')
    new_los_id = request.args.get('new')

    if not old_los_id or not new_los_id:
        return jsonify({'error': 'Missing parameters'}), 400

    try:
        comparison = get_comparison_data_cached(int(old_los_id), int(new_los_id))
        return jsonify(comparison)
    except KeyError:
        return jsonify({'error': 'LOS ID not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio-analysis')
def api_portfolio_analysis():
    """API endpoint for portfolio analysis"""
    try:
        analysis = get_portfolio_analysis()
        return jsonify(analysis)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/drill-down-cases')
def api_drill_down_cases():
    """API endpoint to get specific cases for drill-down from portfolio analysis"""
    try:
        parameter = request.args.get('parameter')
        status_type = request.args.get('status_type')  # improved, degraded, unchanged, no_data
        analysis_type = request.args.get('analysis_type', 'overall')  # overall, approved, rejected

        if not parameter or not status_type:
            return jsonify({'error': 'Missing parameters'}), 400

        # Get portfolio analysis
        portfolio_analysis = get_portfolio_analysis()

        # Find the parameter in the analysis
        param_data = None
        for param in portfolio_analysis[analysis_type]:
            if param['parameter'] == parameter:
                param_data = param
                break

        if not param_data:
            return jsonify({'error': 'Parameter not found'}), 404

        # Get the cases for the requested status type
        case_key = f'{status_type}_cases'
        if case_key not in param_data:
            return jsonify({'error': 'Invalid status type'}), 400

        cases = param_data[case_key]

        # If cases array is empty (from cache), recompute on demand
        if len(cases) == 0 and param_data.get(status_type, 0) > 0:
            print(f"Recomputing cases on demand for {parameter} - {status_type}")

            # Get comparison params to find column name and settings
            comparison_params = get_available_comparison_params()
            column_name = None
            higher_is_better = True
            is_critical = False

            for pname, col, better, critical in comparison_params:
                if pname == parameter:
                    column_name = col
                    higher_is_better = better
                    is_critical = critical
                    break

            if not column_name:
                return jsonify({'error': 'Parameter column not found'}), 404

            # Recompute cases
            filter_status_map = {
                'overall': None,
                'approved': 'approved',
                'rejected': 'rejected'
            }

            recomputed = analyze_parameter_across_portfolio(
                dashboard_cache,
                column_name,
                parameter,
                higher_is_better,
                is_critical,
                filter_status=filter_status_map[analysis_type]
            )

            cases = recomputed[case_key]

        # Enrich cases with full dashboard data
        enriched_cases = []
        for case in cases:
            # Find matching dashboard record
            for dash_record in dashboard_cache:
                if (dash_record['pan'] == case['pan'] and
                        dash_record['old_los_id'] == case['old_los_id'] and
                        dash_record['new_los_id'] == case['new_los_id']):
                    enriched_cases.append(dash_record)
                    break

        return jsonify({
            'parameter': parameter,
            'status_type': status_type,
            'analysis_type': analysis_type,
            'count': len(enriched_cases),
            'cases': enriched_cases
        })

    except Exception as e:
        print(f"Error in drill-down: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/clear-cache')
def api_clear_cache():
    """API endpoint to clear all caches"""
    try:
        clear_all_caches()
        return jsonify({'success': True, 'message': 'All caches cleared. Restart server to recompute.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# HTML TEMPLATE (Same as before, with added cache clear button)
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LOS Comparison Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'IBM Plex Sans', 'Helvetica Neue', Arial, sans-serif;
            background: #f4f4f4;
            color: #161616;
            line-height: 1.6;
        }

        /* Animations */
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        @keyframes slideDown {
            from {
                opacity: 0;
                transform: translateY(-20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes slideUp {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        .fade-in {
            animation: fadeIn 0.3s ease-in;
        }

        .slide-down {
            animation: slideDown 0.4s ease-out;
        }

        .slide-up {
            animation: slideUp 0.4s ease-out;
        }

        .header {
            background: #ffffff;
            border-bottom: 1px solid #e0e0e0;
            padding: 1rem 2rem;
            animation: slideDown 0.5s ease-out;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
        }

        .header-left {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .header-logo {
            height: 20px;
            width: auto;
        }

        .header h1 {
            font-size: 1.5rem;
            font-weight: 400;
            color: #161616;
            margin: 0;
        }

        .cache-info {
            font-size: 0.75rem;
            color: #525252;
            display: flex;
            gap: 0.5rem;
            align-items: center;
        }

        .cache-badge {
            padding: 0.25rem 0.5rem;
            background: #d2f4ea;
            color: #0e6027;
            border-radius: 2px;
            font-weight: 600;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }

        .search-section {
            background: #ffffff;
            padding: 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
            animation: slideDown 0.6s ease-out;
        }

        .search-bar {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }

        .filter-bar {
            display: flex;
            gap: 0.5rem;
            align-items: center;
        }

        .filter-label {
            font-size: 0.875rem;
            color: #525252;
            font-weight: 500;
        }

        .search-input, .filter-select {
            flex: 1;
            padding: 0.75rem 1rem;
            border: 1px solid #8d8d8d;
            font-size: 0.875rem;
            outline: none;
            font-family: inherit;
            transition: all 0.2s ease;
        }

        .filter-select {
            flex: 0 0 200px;
            cursor: pointer;
            background: #ffffff;
        }

        .search-input:focus, .filter-select:focus {
            border-color: #0f62fe;
            outline: 2px solid #0f62fe;
            outline-offset: -2px;
        }

        .btn {
            padding: 0.75rem 1.5rem;
            background: #0f62fe;
            color: #ffffff;
            border: none;
            font-size: 0.875rem;
            cursor: pointer;
            font-family: inherit;
            font-weight: 400;
            transition: all 0.2s ease;
            position: relative;
            overflow: hidden;
        }

        .btn:hover {
            background: #0353e9;
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .btn:active {
            transform: translateY(0);
        }

        .btn-secondary {
            background: #393939;
        }

        .btn-secondary:hover {
            background: #4c4c4c;
        }

        .btn-danger {
            background: #da1e28;
            padding: 0.5rem 1rem;
            font-size: 0.75rem;
        }

        .btn-danger:hover {
            background: #ba1b23;
        }

        .stats {
            background: #ffffff;
            padding: 1rem 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
            display: flex;
            gap: 2rem;
            font-size: 0.875rem;
            animation: slideDown 0.7s ease-out;
        }

        .stat-item {
            color: #525252;
        }

        .stat-value {
            font-weight: 600;
            color: #161616;
            margin-left: 0.5rem;
        }

        .view-toggle {
            background: #ffffff;
            padding: 1rem 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
            animation: slideDown 0.8s ease-out;
        }

        .tab-buttons {
            display: flex;
            gap: 0.5rem;
        }

        .tab-btn {
            padding: 0.5rem 1rem;
            background: transparent;
            border: 1px solid #8d8d8d;
            color: #161616;
            cursor: pointer;
            font-size: 0.875rem;
            font-family: inherit;
            transition: all 0.2s ease;
        }

        .tab-btn:hover {
            background: #f4f4f4;
        }

        .tab-btn.active {
            background: #e0e0e0;
            border-color: #e0e0e0;
        }

        .view {
            display: none;
        }

        .view.active {
            display: block;
        }

        table {
            width: 100%;
            background: #ffffff;
            border: 1px solid #e0e0e0;
            border-collapse: collapse;
            font-size: 0.875rem;
        }

        thead {
            background: #e0e0e0;
        }

        th {
            text-align: left;
            padding: 0.75rem 1rem;
            font-weight: 600;
            color: #161616;
            border-bottom: 1px solid #8d8d8d;
        }

        td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #e0e0e0;
            transition: background 0.2s ease;
        }

        tbody tr {
            transition: all 0.2s ease;
        }

        tbody tr:hover {
            background: #f4f4f4;
            transform: scale(1.001);
        }

        .status {
            display: inline-block;
            padding: 0.125rem 0.5rem;
            font-size: 0.75rem;
            border-radius: 0;
            transition: all 0.2s ease;
        }

        .status-approved { background: #d2f4ea; color: #0e6027; }
        .status-rejected { background: #ffd7d9; color: #750e13; }
        .status-closed { background: #d0e2ff; color: #002d9c; }
        .status-inprogress { background: #fcf4d6; color: #684e00; }
        .status-migrated { background: #e0e0e0; color: #161616; }

        /* Status badges for comparison table */
        .status-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            border-radius: 2px;
        }

        .status-badge.improved {
            background: #d2f4ea;
            color: #0e6027;
        }

        .status-badge.degraded {
            background: #ffd7d9;
            color: #750e13;
        }

        .status-badge.unchanged {
            background: #e0e0e0;
            color: #525252;
        }

        .status-badge.no-data {
            background: #f4f4f4;
            color: #8d8d8d;
        }

        .compare-btn {
            padding: 0.375rem 0.75rem;
            background: #0f62fe;
            color: #ffffff;
            border: none;
            cursor: pointer;
            font-size: 0.75rem;
            font-family: inherit;
            transition: all 0.2s ease;
        }

        .compare-btn:hover {
            background: #0353e9;
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(15, 98, 254, 0.3);
        }

        .compare-btn:active {
            transform: translateY(0);
        }

        /* Spinner Loader */
        .loading {
            text-align: center;
            padding: 3rem;
            color: #525252;
            background: #ffffff;
            border: 1px solid #e0e0e0;
        }

        .spinner {
            border: 3px solid #e0e0e0;
            border-top: 3px solid #0f62fe;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 0.8s linear infinite;
            margin: 0 auto 1rem;
        }

        .comparison-header {
            background: #ffffff;
            padding: 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
            animation: slideDown 0.4s ease-out;
        }

        .comparison-info {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-top: 1rem;
        }

        .info-item {
            padding: 0.75rem;
            background: #f4f4f4;
            border-left: 3px solid #0f62fe;
            transition: all 0.3s ease;
        }

        .info-item:hover {
            background: #e0e0e0;
            transform: translateX(4px);
        }

        .info-label {
            font-size: 0.75rem;
            color: #525252;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .info-value {
            font-size: 1rem;
            font-weight: 600;
            margin-top: 0.25rem;
        }

        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }

        .summary-card {
            background: #ffffff;
            padding: 1rem;
            border: 1px solid #e0e0e0;
            text-align: center;
            transition: all 0.3s ease;
        }

        .summary-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }

        .summary-label {
            font-size: 0.75rem;
            color: #525252;
            text-transform: uppercase;
        }

        .summary-value {
            font-size: 2rem;
            font-weight: 600;
            margin-top: 0.5rem;
        }

        .summary-card.improved { border-left: 4px solid #24a148; }
        .summary-card.degraded { border-left: 4px solid #da1e28; }
        .summary-card.unchanged { border-left: 4px solid #0f62fe; }

        .change-improved { color: #24a148; }
        .change-degraded { color: #da1e28; }
        .change-unchanged { color: #525252; }
        .change-no-data { color: #8d8d8d; }

        .mono {
            font-family: 'IBM Plex Mono', 'Courier New', monospace;
        }

        /* Critical Parameter Badges */
        .critical-badge {
            display: inline-block;
            padding: 0.125rem 0.375rem;
            background: #da1e28;
            color: #ffffff;
            font-size: 0.625rem;
            font-weight: 700;
            text-transform: uppercase;
            margin-left: 0.5rem;
            border-radius: 2px;
            letter-spacing: 0.5px;
        }

        .highly-critical-row {
            background: #fff1f1 !important;
            border-left: 4px solid #da1e28 !important;
        }

        .highly-critical-row:hover {
            background: #ffe0e0 !important;
        }

        .critical-alert {
            background: #fff1f1;
            border-left: 4px solid #da1e28;
            padding: 1rem;
            margin-bottom: 1rem;
            animation: slideDown 0.4s ease-out;
        }

        .critical-alert-title {
            font-size: 0.875rem;
            font-weight: 600;
            color: #750e13;
            margin-bottom: 0.5rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .critical-alert-icon {
            font-size: 1.25rem;
        }

        .critical-alert-text {
            font-size: 0.75rem;
            color: #750e13;
        }

        /* Clickable cells in portfolio table */
        .clickable-cell {
            cursor: pointer;
            text-decoration: underline;
            transition: all 0.2s ease;
        }

        .clickable-cell:hover {
            color: #0f62fe;
            font-weight: 600;
        }

        /* Drill-down modal */
        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.5);
            animation: fadeIn 0.3s ease-in;
        }

        .modal-content {
            background-color: #ffffff;
            margin: 5% auto;
            padding: 0;
            border: 1px solid #8d8d8d;
            width: 90%;
            max-width: 1200px;
            max-height: 80vh;
            overflow: auto;
            animation: slideDown 0.4s ease-out;
        }

        .modal-header {
            background: #e0e0e0;
            padding: 1rem 1.5rem;
            border-bottom: 1px solid #8d8d8d;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .modal-title {
            font-size: 1.125rem;
            font-weight: 600;
        }

        .modal-close {
            font-size: 1.5rem;
            font-weight: 700;
            color: #8d8d8d;
            cursor: pointer;
            background: none;
            border: none;
            padding: 0;
            line-height: 1;
        }

        .modal-close:hover {
            color: #161616;
        }

        .modal-body {
            padding: 1.5rem;
        }

        /* Pagination Styles */
        .pagination-container {
            background: #ffffff;
            padding: 1rem 1.5rem;
            margin-top: 1rem;
            border: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
            animation: slideUp 0.5s ease-out;
        }

        .pagination-info {
            font-size: 0.875rem;
            color: #525252;
        }

        .pagination-controls {
            display: flex;
            gap: 0.5rem;
            align-items: center;
        }

        .page-btn {
            padding: 0.5rem 0.75rem;
            background: #ffffff;
            border: 1px solid #8d8d8d;
            color: #161616;
            cursor: pointer;
            font-size: 0.875rem;
            font-family: inherit;
            transition: all 0.2s ease;
            min-width: 40px;
        }

        .page-btn:hover:not(:disabled) {
            background: #e0e0e0;
            border-color: #0f62fe;
        }

        .page-btn.active {
            background: #0f62fe;
            color: #ffffff;
            border-color: #0f62fe;
        }

        .page-btn:disabled {
            opacity: 0.4;
            cursor: not-allowed;
        }

        .page-ellipsis {
            padding: 0.5rem;
            color: #525252;
        }

        /* Portfolio Analysis Styles */
        .portfolio-header {
            background: #ffffff;
            padding: 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
        }

        .portfolio-header h2 {
            font-size: 1.25rem;
            font-weight: 400;
            margin-bottom: 0.5rem;
        }

        .portfolio-header p {
            color: #525252;
            font-size: 0.875rem;
        }

        .chart-container {
            background: #ffffff;
            padding: 2rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
            position: relative;
            height: 500px;
        }

        .chart-title {
            font-size: 1rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: #161616;
        }

        .chart-wrapper {
            position: relative;
            height: calc(100% - 2rem);
        }

        .analysis-filters {
            background: #ffffff;
            padding: 1rem 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid #e0e0e0;
            display: flex;
            gap: 1rem;
            align-items: center;
        }

        @media (max-width: 768px) {
            .container {
                padding: 1rem;
            }

            .search-bar, .filter-bar {
                flex-direction: column;
            }

            .filter-select {
                flex: 1;
            }

            table {
                font-size: 0.75rem;
            }

            th, td {
                padding: 0.5rem;
            }

            .pagination-container {
                flex-direction: column;
                gap: 1rem;
            }

            .pagination-controls {
                flex-wrap: wrap;
                justify-content: center;
            }

            .chart-container {
                height: 400px;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="header-left">
            <img src="https://famescore.in/assets/images/fameImages/fameScoreLogo.png" alt="Fame Score Logo" class="header-logo">
            <h1>Fame Health Dashboard</h1>
        </div>
        <div class="cache-info">
            <span class="cache-badge">⚡ CACHED</span>
            <span>Instant load enabled</span>
        </div>
    </div>

    <div class="container">
        <div class="search-section" id="searchSection">
            <div class="search-bar">
                <input type="text" id="searchInput" class="search-input" 
                       placeholder="Search by PAN, Old LOS ID, or New LOS ID">
                <button class="btn" onclick="searchData()">Search</button>
                <button class="btn btn-secondary" onclick="clearSearch()">Clear</button>
            </div>
            <div class="filter-bar">
                <span class="filter-label">Filter by Status:</span>
                <select id="statusFilter" class="filter-select" onchange="applyFilters()">
                    <option value="">All Statuses</option>
                </select>
            </div>
        </div>

        <div class="stats" id="statsBar">
            <div class="stat-item">
                Total Records: <span class="stat-value" id="totalRecords">0</span>
            </div>
            <div class="stat-item">
                Showing: <span class="stat-value" id="showingRecords">0</span>
            </div>
            <div class="stat-item" id="pageStatItem">
                Page: <span class="stat-value" id="currentPageDisplay">1</span>
            </div>
        </div>

        <div class="view-toggle">
            <div class="tab-buttons">
                <button class="tab-btn active" onclick="switchView('dashboard')">Dashboard</button>
                <button class="tab-btn" onclick="switchView('portfolio')">Portfolio Analysis</button>
                <button class="tab-btn" id="compareTab" onclick="switchView('comparison')" style="display:none;">
                    Comparison
                </button>
            </div>
        </div>

        <div id="dashboardView" class="view active">
            <div id="loadingIndicator" class="loading">
                <div class="spinner"></div>
                <div>Loading data...</div>
            </div>
            <div id="tableContainer" style="display:none;">
                <table>
                    <thead>
                        <tr>
                            <th>PAN</th>
                            <th>Old LOS ID</th>
                            <th>Old Program</th>
                            <th>Old Status</th>
                            <th>New LOS ID</th>
                            <th>New Program</th>
                            <th>New Creation Date</th>
                            <th>New Status</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody id="tableBody"></tbody>
                </table>
                <div class="pagination-container">
                    <div class="pagination-info" id="paginationInfo"></div>
                    <div class="pagination-controls" id="paginationControls"></div>
                </div>
            </div>
        </div>

        <div id="portfolioView" class="view">
            <div id="portfolioLoading" class="loading" style="display:none;">
                <div class="spinner"></div>
                <div>Loading portfolio analysis...</div>
            </div>
            <div id="portfolioContent" style="display:none;">
                <div class="portfolio-header">
                    <h2>Portfolio Analysis</h2>
                    <p>Comprehensive analysis of parameter changes across all cases in the portfolio</p>
                </div>

                <div class="analysis-filters">
                    <span class="filter-label">View:</span>
                    <select id="analysisTypeFilter" class="filter-select" onchange="updatePortfolioCharts()">
                        <option value="overall">Overall Portfolio</option>
                        <option value="approved">Approved Cases Only</option>
                        <option value="rejected">Rejected Cases Only</option>
                    </select>
                </div>

                <div class="summary-cards" id="portfolioSummaryCards"></div>

                <div class="chart-container">
                    <div class="chart-title">Parameter Improvement vs Degradation Rates</div>
                    <div class="chart-wrapper">
                        <canvas id="improvementChart"></canvas>
                    </div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">Average Change by Parameter</div>
                    <div class="chart-wrapper">
                        <canvas id="avgChangeChart"></canvas>
                    </div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">Old vs New Average Values</div>
                    <div class="chart-wrapper">
                        <canvas id="oldVsNewChart"></canvas>
                    </div>
                </div>

                <table>
                    <thead>
                        <tr>
                            <th>Parameter</th>
                            <th>Total Cases</th>
                            <th>Improved</th>
                            <th>Degraded</th>
                            <th>Unchanged</th>
                            <th>No Data</th>
                            <th>Avg Change</th>
                            <th>Improvement Rate</th>
                        </tr>
                    </thead>
                    <tbody id="portfolioTableBody"></tbody>
                </table>
            </div>
        </div>

        <div id="comparisonView" class="view">
            <div id="comparisonLoading" class="loading" style="display:none;">
                <div class="spinner"></div>
                <div>Loading comparison...</div>
            </div>
            <div id="comparisonContent" style="display:none;">
                <div class="comparison-header">
                    <button class="btn btn-secondary" onclick="switchView('dashboard')">← Back to Dashboard</button>
                    <div class="comparison-info" id="comparisonInfo"></div>
                </div>

                <div class="summary-cards" id="summaryCards"></div>

                <table id="comparisonTable">
                    <thead>
                        <tr>
                            <th>Parameter</th>
                            <th id="oldDateHeader">Old Value</th>
                            <th id="newDateHeader">New Value</th>
                            <th>Change</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody id="comparisonTableBody"></tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Drill-Down Modal -->
    <div id="drillDownModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <div class="modal-title" id="modalTitle">Cases</div>
                <button class="modal-close" onclick="closeDrillDown()">&times;</button>
            </div>
            <div class="modal-body">
                <div id="modalLoading" class="loading" style="display:none;">
                    <div class="spinner"></div>
                    <div>Loading cases...</div>
                </div>
                <div id="modalContent" style="display:none;">
                    <div style="margin-bottom: 1rem; padding: 1rem; background: #f4f4f4; border-left: 3px solid #0f62fe;">
                        <div style="font-size: 0.875rem; color: #525252;">
                            Showing <strong id="drillDownCount">0</strong> cases
                        </div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>PAN</th>
                                <th>Old LOS ID</th>
                                <th>Old Program</th>
                                <th>Old Status</th>
                                <th>New LOS ID</th>
                                <th>New Program</th>
                                <th>New Creation Date</th>
                                <th>New Status</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody id="modalTableBody"></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
        let allData = [];
        let filteredData = [];
        let currentPage = 1;
        const recordsPerPage = 20;
        let portfolioData = null;
        let charts = {};

        window.addEventListener('DOMContentLoaded', () => {
            loadDashboardData();
            document.getElementById('searchInput').addEventListener('keypress', (e) => {
                if (e.key === 'Enter') searchData();
            });
        });

        async function loadDashboardData(search = '') {
            const loading = document.getElementById('loadingIndicator');
            const container = document.getElementById('tableContainer');

            loading.style.display = 'block';
            container.style.display = 'none';

            try {
                const url = search ? `/api/dashboard?search=${encodeURIComponent(search)}` : '/api/dashboard';
                const response = await fetch(url);
                const result = await response.json();

                allData = result.data;

                // Populate status filter
                populateStatusFilter(allData);

                // Apply filters and render
                applyFilters();
                updateStats(result.total);

                container.style.display = 'block';
                container.classList.add('fade-in');
            } catch (error) {
                loading.innerHTML = '<div>Error loading data</div>';
            } finally {
                loading.style.display = 'none';
            }
        }

        async function loadPortfolioAnalysis() {
            const loading = document.getElementById('portfolioLoading');
            const content = document.getElementById('portfolioContent');

            loading.style.display = 'block';
            content.style.display = 'none';

            try {
                const response = await fetch('/api/portfolio-analysis');
                portfolioData = await response.json();

                renderPortfolioAnalysis();
                content.style.display = 'block';
                content.classList.add('fade-in');
                loading.style.display = 'none';
            } catch (error) {
                loading.innerHTML = '<div>Error loading portfolio analysis</div>';
            }
        }

        function renderPortfolioAnalysis() {
            const analysisType = document.getElementById('analysisTypeFilter').value;
            const data = portfolioData[analysisType];

            // Render summary cards
            const totalCases = data.reduce((sum, param) => sum + param.total_cases, 0) / data.length;
            const totalImproved = data.reduce((sum, param) => sum + param.improved, 0);
            const totalDegraded = data.reduce((sum, param) => sum + param.degraded, 0);
            const totalUnchanged = data.reduce((sum, param) => sum + param.unchanged, 0);
            const totalNoData = data.reduce((sum, param) => sum + param.no_data, 0);

            const summaryCards = document.getElementById('portfolioSummaryCards');
            summaryCards.innerHTML = `
                <div class="summary-card improved">
                    <div class="summary-label">Total Improvements</div>
                    <div class="summary-value">${totalImproved}</div>
                </div>
                <div class="summary-card degraded">
                    <div class="summary-label">Total Degradations</div>
                    <div class="summary-value">${totalDegraded}</div>
                </div>
                <div class="summary-card unchanged">
                    <div class="summary-label">Unchanged</div>
                    <div class="summary-value">${totalUnchanged}</div>
                </div>
                <div class="summary-card">
                    <div class="summary-label">No Data Available</div>
                    <div class="summary-value">${totalNoData}</div>
                </div>
                <div class="summary-card">
                    <div class="summary-label">Parameters Analyzed</div>
                    <div class="summary-value">${data.length}</div>
                </div>
            `;

            // Render table
            renderPortfolioTable(data);

            // Render charts
            updatePortfolioCharts();
        }

        function renderPortfolioTable(data) {
            const tbody = document.getElementById('portfolioTableBody');
            tbody.innerHTML = '';

            data.forEach((param, index) => {
                const tr = document.createElement('tr');
                tr.style.animationDelay = `${index * 0.02}s`;
                tr.classList.add('fade-in');

                const improvementRate = param.improvement_rate;
                const improvementClass = improvementRate > 50 ? 'change-improved' : 
                                        improvementRate < 30 ? 'change-degraded' : 'change-unchanged';

                // Add critical badge to parameter name
                let parameterDisplay = param.parameter;
                if (param.is_critical) {
                    parameterDisplay += '<span class="critical-badge">CRITICAL</span>';
                }

                const analysisType = document.getElementById('analysisTypeFilter').value;

                tr.innerHTML = `
                    <td>${parameterDisplay}</td>
                    <td class="mono">${param.total_cases}</td>
                    <td class="change-improved mono clickable-cell" onclick="showDrillDown('${param.parameter.replace(/'/g, "\\'")}', 'improved', '${analysisType}')">${param.improved}</td>
                    <td class="change-degraded mono clickable-cell" onclick="showDrillDown('${param.parameter.replace(/'/g, "\\'")}', 'degraded', '${analysisType}')">${param.degraded}</td>
                    <td class="mono clickable-cell" onclick="showDrillDown('${param.parameter.replace(/'/g, "\\'")}', 'unchanged', '${analysisType}')">${param.unchanged}</td>
                    <td class="change-no-data mono clickable-cell" onclick="showDrillDown('${param.parameter.replace(/'/g, "\\'")}', 'no_data', '${analysisType}')">${param.no_data}</td>
                    <td class="mono">${param.avg_change}</td>
                    <td class="mono ${improvementClass}">${param.improvement_rate}%</td>
                `;
                tbody.appendChild(tr);
            });
        }

        function updatePortfolioCharts() {
            if (!portfolioData) return;

            const analysisType = document.getElementById('analysisTypeFilter').value;
            const data = portfolioData[analysisType];

            renderPortfolioTable(data);

            // Destroy existing charts
            Object.values(charts).forEach(chart => chart.destroy());
            charts = {};

            // Chart 1: Improvement vs Degradation Rates
            const ctx1 = document.getElementById('improvementChart').getContext('2d');
            charts.improvement = new Chart(ctx1, {
                type: 'bar',
                data: {
                    labels: data.map(p => p.parameter),
                    datasets: [
                        {
                            label: 'Improvement Rate %',
                            data: data.map(p => p.improvement_rate),
                            backgroundColor: 'rgba(36, 161, 72, 0.7)',
                            borderColor: 'rgba(36, 161, 72, 1)',
                            borderWidth: 1
                        },
                        {
                            label: 'Degradation Rate %',
                            data: data.map(p => p.degradation_rate),
                            backgroundColor: 'rgba(218, 30, 40, 0.7)',
                            borderColor: 'rgba(218, 30, 40, 1)',
                            borderWidth: 1
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'top',
                        }
                    },
                    scales: {
                        x: {
                            ticks: {
                                maxRotation: 90,
                                minRotation: 45,
                                font: {
                                    size: 10
                                }
                            }
                        },
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Percentage (%)'
                            }
                        }
                    }
                }
            });

            // Chart 2: Average Change
            const ctx2 = document.getElementById('avgChangeChart').getContext('2d');
            charts.avgChange = new Chart(ctx2, {
                type: 'bar',
                data: {
                    labels: data.map(p => p.parameter),
                    datasets: [{
                        label: 'Average Change',
                        data: data.map(p => p.avg_change),
                        backgroundColor: data.map(p => p.avg_change >= 0 ? 'rgba(36, 161, 72, 0.7)' : 'rgba(218, 30, 40, 0.7)'),
                        borderColor: data.map(p => p.avg_change >= 0 ? 'rgba(36, 161, 72, 1)' : 'rgba(218, 30, 40, 1)'),
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: false
                        }
                    },
                    scales: {
                        x: {
                            ticks: {
                                maxRotation: 90,
                                minRotation: 45,
                                font: {
                                    size: 10
                                }
                            }
                        },
                        y: {
                            title: {
                                display: true,
                                text: 'Average Change'
                            }
                        }
                    }
                }
            });

            // Chart 3: Old vs New Values
            const ctx3 = document.getElementById('oldVsNewChart').getContext('2d');
            charts.oldVsNew = new Chart(ctx3, {
                type: 'line',
                data: {
                    labels: data.map(p => p.parameter),
                    datasets: [
                        {
                            label: 'Average Old Value',
                            data: data.map(p => p.avg_old_value),
                            borderColor: 'rgba(15, 98, 254, 1)',
                            backgroundColor: 'rgba(15, 98, 254, 0.1)',
                            tension: 0.1,
                            fill: true
                        },
                        {
                            label: 'Average New Value',
                            data: data.map(p => p.avg_new_value),
                            borderColor: 'rgba(57, 57, 57, 1)',
                            backgroundColor: 'rgba(57, 57, 57, 0.1)',
                            tension: 0.1,
                            fill: true
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'top',
                        }
                    },
                    scales: {
                        x: {
                            ticks: {
                                maxRotation: 90,
                                minRotation: 45,
                                font: {
                                    size: 10
                                }
                            }
                        },
                        y: {
                            title: {
                                display: true,
                                text: 'Value'
                            }
                        }
                    }
                }
            });
        }

        function populateStatusFilter(data) {
            const statusFilter = document.getElementById('statusFilter');
            const statuses = new Set();

            data.forEach(row => {
                statuses.add(row.new_status);
            });

            // Clear existing options except "All Statuses"
            statusFilter.innerHTML = '<option value="">All Statuses</option>';

            // Add sorted unique statuses
            Array.from(statuses).sort().forEach(status => {
                const option = document.createElement('option');
                option.value = status;
                option.textContent = status;
                statusFilter.appendChild(option);
            });
        }

        function applyFilters() {
            const statusFilter = document.getElementById('statusFilter').value;

            filteredData = allData.filter(row => {
                if (statusFilter && row.new_status !== statusFilter) {
                    return false;
                }
                return true;
            });

            currentPage = 1;
            renderPage();
        }

        function renderPage() {
            const startIndex = (currentPage - 1) * recordsPerPage;
            const endIndex = startIndex + recordsPerPage;
            const pageData = filteredData.slice(startIndex, endIndex);

            renderTable(pageData);
            renderPagination();
            updateShowingCount();
        }

        function renderTable(data) {
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';

            data.forEach((row, index) => {
                const tr = document.createElement('tr');
                tr.style.animationDelay = `${index * 0.02}s`;
                tr.classList.add('fade-in');

                tr.innerHTML = `
                    <td class="mono">${row.pan}</td>
                    <td class="mono">${row.old_los_id}</td>
                    <td>${row.old_program}</td>
                    <td>${getStatusBadge(row.old_status)}</td>
                    <td class="mono">${row.new_los_id}</td>
                    <td>${row.new_program}</td>
                    <td>${row.new_created}</td>
                    <td>${getStatusBadge(row.new_status)}</td>
                    <td>
                        <button class="compare-btn" onclick="loadComparison(${row.old_los_id}, ${row.new_los_id})">
                            Compare
                        </button>
                    </td>
                `;
                tbody.appendChild(tr);
            });
        }

        function renderPagination() {
            const totalPages = Math.ceil(filteredData.length / recordsPerPage);
            const controls = document.getElementById('paginationControls');
            controls.innerHTML = '';

            // First button
            const firstBtn = createPageButton('First', 1, currentPage === 1);
            controls.appendChild(firstBtn);

            // Previous button
            const prevBtn = createPageButton('‹', currentPage - 1, currentPage === 1);
            controls.appendChild(prevBtn);

            // Page numbers
            const pagesToShow = getPageNumbers(currentPage, totalPages);
            pagesToShow.forEach(page => {
                if (page === '...') {
                    const ellipsis = document.createElement('span');
                    ellipsis.className = 'page-ellipsis';
                    ellipsis.textContent = '...';
                    controls.appendChild(ellipsis);
                } else {
                    const pageBtn = createPageButton(page, page, false, page === currentPage);
                    controls.appendChild(pageBtn);
                }
            });

            // Next button
            const nextBtn = createPageButton('›', currentPage + 1, currentPage === totalPages);
            controls.appendChild(nextBtn);

            // Last button
            const lastBtn = createPageButton('Last', totalPages, currentPage === totalPages);
            controls.appendChild(lastBtn);

            // Update pagination info
            const startRecord = (currentPage - 1) * recordsPerPage + 1;
            const endRecord = Math.min(currentPage * recordsPerPage, filteredData.length);
            document.getElementById('paginationInfo').textContent = 
                `Showing ${startRecord}-${endRecord} of ${filteredData.length} records`;
        }

        function createPageButton(text, page, disabled, active = false) {
            const btn = document.createElement('button');
            btn.className = 'page-btn' + (active ? ' active' : '');
            btn.textContent = text;
            btn.disabled = disabled;
            if (!disabled) {
                btn.onclick = () => goToPage(page);
            }
            return btn;
        }

        function getPageNumbers(current, total) {
            const pages = [];
            const delta = 2;

            for (let i = 1; i <= total; i++) {
                if (i === 1 || i === total || (i >= current - delta && i <= current + delta)) {
                    pages.push(i);
                } else if (pages[pages.length - 1] !== '...') {
                    pages.push('...');
                }
            }

            return pages;
        }

        function goToPage(page) {
            currentPage = page;
            renderPage();
            document.getElementById('currentPageDisplay').textContent = currentPage;
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }

        function getStatusBadge(status) {
            let className = 'status';
            if (status.toLowerCase().includes('approved')) className += ' status-approved';
            else if (status.toLowerCase().includes('rejected')) className += ' status-rejected';
            else if (status.toLowerCase().includes('closed')) className += ' status-closed';
            else if (status.toLowerCase().includes('progress')) className += ' status-inprogress';
            else if (status.toLowerCase().includes('migrated')) className += ' status-migrated';

            return `<span class="${className}">${status}</span>`;
        }

        function updateStats(total) {
            document.getElementById('totalRecords').textContent = total.toLocaleString();
        }

        function updateShowingCount() {
            document.getElementById('showingRecords').textContent = filteredData.length.toLocaleString();
        }

        function searchData() {
            const query = document.getElementById('searchInput').value;
            loadDashboardData(query);
        }

        function clearSearch() {
            document.getElementById('searchInput').value = '';
            document.getElementById('statusFilter').value = '';
            loadDashboardData();
        }

        async function loadComparison(oldId, newId) {
            const loading = document.getElementById('comparisonLoading');
            const content = document.getElementById('comparisonContent');
            const tab = document.getElementById('compareTab');

            switchView('comparison');
            tab.style.display = 'block';

            loading.style.display = 'block';
            content.style.display = 'none';

            try {
                const response = await fetch(`/api/compare?old=${oldId}&new=${newId}`);
                const data = await response.json();

                renderComparison(data);
                content.style.display = 'block';
                content.classList.add('fade-in');
            } catch (error) {
                loading.innerHTML = '<div>Error loading comparison</div>';
            } finally {
                loading.style.display = 'none';
            }
        }

        function renderComparison(data) {
            // Update table headers with creation dates
            document.getElementById('oldDateHeader').textContent = `Old LOS (${data.old_created})`;
            document.getElementById('newDateHeader').textContent = `New LOS (${data.new_created})`;

            const info = document.getElementById('comparisonInfo');
            info.innerHTML = `
                <div class="info-item">
                    <div class="info-label">PAN</div>
                    <div class="info-value mono">${data.pan}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Old LOS ID</div>
                    <div class="info-value mono">${data.old_los_id}</div>
                    <div style="font-size:0.75rem;margin-top:0.25rem;">${data.old_program}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">New LOS ID</div>
                    <div class="info-value mono">${data.new_los_id}</div>
                    <div style="font-size:0.75rem;margin-top:0.25rem;">${data.new_program}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Old Status</div>
                    <div class="info-value">${data.old_status}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">New Status</div>
                    <div class="info-value">${data.new_status}</div>
                </div>
            `;

            const stats = calculateStats(data.comparisons);
            const cards = document.getElementById('summaryCards');

            // Add critical alert if present
            let criticalAlert = '';
            if (data.has_critical_case) {
                criticalAlert = `
                    <div class="critical-alert">
                        <div class="critical-alert-title">
                            <span>HIGHLY CRITICAL CASE DETECTED</span>
                        </div>
                        <div class="critical-alert-text">
                            This case contains critical parameter changes (CIBIL drop ≥50 points or CMR increase ≥2 points)
                        </div>
                    </div>
                `;
            }

            cards.innerHTML = criticalAlert + `
                <div class="summary-card improved">
                    <div class="summary-label">Improved</div>
                    <div class="summary-value">${stats.improved}</div>
                </div>
                <div class="summary-card degraded">
                    <div class="summary-label">Degraded</div>
                    <div class="summary-value">${stats.degraded}</div>
                </div>
                <div class="summary-card unchanged">
                    <div class="summary-label">Unchanged</div>
                    <div class="summary-value">${stats.unchanged}</div>
                </div>
                <div class="summary-card">
                    <div class="summary-label">No Data</div>
                    <div class="summary-value">${stats.noData}</div>
                </div>
            `;

            const tbody = document.getElementById('comparisonTableBody');
            tbody.innerHTML = '';

            data.comparisons.forEach((comp, index) => {
                const tr = document.createElement('tr');
                tr.style.animationDelay = `${index * 0.02}s`;
                tr.classList.add('fade-in');

                // Highlight highly critical rows
                if (comp.is_highly_critical) {
                    tr.classList.add('highly-critical-row');
                }

                const statusClass = `change-${comp.status}`;

                // Create status badge text
                let statusText = 'No Data';
                let badgeClass = 'no-data';
                if (comp.status === 'improved') {
                    statusText = 'Improved';
                    badgeClass = 'improved';
                } else if (comp.status === 'degraded') {
                    statusText = 'Degraded';
                    badgeClass = 'degraded';
                } else if (comp.status === 'unchanged') {
                    statusText = 'Unchanged';
                    badgeClass = 'unchanged';
                }

                // Add critical badge to parameter name if critical
                let parameterDisplay = comp.parameter;
                if (comp.is_critical) {
                    parameterDisplay += '<span class="critical-badge">CRITICAL</span>';
                }

                tr.innerHTML = `
                    <td>${parameterDisplay}</td>
                    <td class="mono">${comp.old_value}</td>
                    <td class="mono">${comp.new_value}</td>
                    <td class="mono ${statusClass}">${comp.change}</td>
                    <td><span class="status-badge ${badgeClass}">${statusText}</span></td>
                `;
                tbody.appendChild(tr);
            });
        }

        function calculateStats(comparisons) {
            return comparisons.reduce((acc, comp) => {
                acc[comp.status === 'improved' ? 'improved' :
                    comp.status === 'degraded' ? 'degraded' :
                    comp.status === 'unchanged' ? 'unchanged' : 'noData']++;
                return acc;
            }, { improved: 0, degraded: 0, unchanged: 0, noData: 0 });
        }

        function switchView(view) {
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));

            const pageStatItem = document.getElementById('pageStatItem');
            const searchSection = document.getElementById('searchSection');
            const statsBar = document.getElementById('statsBar');

            if (view === 'dashboard') {
                document.getElementById('dashboardView').classList.add('active');
                document.querySelectorAll('.tab-btn')[0].classList.add('active');
                // Show search section, stats bar, and page number in dashboard
                if (searchSection) searchSection.style.display = 'block';
                if (statsBar) statsBar.style.display = 'flex';
                if (pageStatItem) pageStatItem.style.display = 'block';
            } else if (view === 'portfolio') {
                document.getElementById('portfolioView').classList.add('active');
                document.querySelectorAll('.tab-btn')[1].classList.add('active');
                // Hide search section, stats bar, and page number in portfolio view
                if (searchSection) searchSection.style.display = 'none';
                if (statsBar) statsBar.style.display = 'none';
                if (pageStatItem) pageStatItem.style.display = 'none';
                // Load portfolio analysis if not already loaded
                if (!portfolioData) {
                    loadPortfolioAnalysis();
                }
            } else {
                document.getElementById('comparisonView').classList.add('active');
                document.getElementById('compareTab').classList.add('active');
                // Hide search section, stats bar, and page number in comparison view
                if (searchSection) searchSection.style.display = 'none';
                if (statsBar) statsBar.style.display = 'none';
                if (pageStatItem) pageStatItem.style.display = 'none';
            }
        }

        // Drill-Down Functions
        async function showDrillDown(parameter, statusType, analysisType) {
            const modal = document.getElementById('drillDownModal');
            const loading = document.getElementById('modalLoading');
            const content = document.getElementById('modalContent');
            const title = document.getElementById('modalTitle');

            // Set modal title
            const statusLabel = statusType === 'improved' ? 'Improved' :
                              statusType === 'degraded' ? 'Degraded' :
                              statusType === 'unchanged' ? 'Unchanged' : 'No Data';

            title.textContent = `${parameter} - ${statusLabel} Cases`;

            // Show modal and loading
            modal.style.display = 'block';
            loading.style.display = 'block';
            content.style.display = 'none';

            try {
                const response = await fetch(`/api/drill-down-cases?parameter=${encodeURIComponent(parameter)}&status_type=${statusType}&analysis_type=${analysisType}`);
                const data = await response.json();

                if (data.error) {
                    alert('Error loading cases: ' + data.error);
                    closeDrillDown();
                    return;
                }

                // Render cases in modal
                renderDrillDownCases(data.cases);
                document.getElementById('drillDownCount').textContent = data.count;

                content.style.display = 'block';
                loading.style.display = 'none';
            } catch (error) {
                alert('Error loading drill-down data');
                closeDrillDown();
            }
        }

        function renderDrillDownCases(cases) {
            const tbody = document.getElementById('modalTableBody');
            tbody.innerHTML = '';

            cases.forEach((row, index) => {
                const tr = document.createElement('tr');
                tr.style.animationDelay = `${index * 0.02}s`;
                tr.classList.add('fade-in');

                tr.innerHTML = `
                    <td class="mono">${row.pan}</td>
                    <td class="mono">${row.old_los_id}</td>
                    <td>${row.old_program}</td>
                    <td>${getStatusBadge(row.old_status)}</td>
                    <td class="mono">${row.new_los_id}</td>
                    <td>${row.new_program}</td>
                    <td>${row.new_created}</td>
                    <td>${getStatusBadge(row.new_status)}</td>
                    <td>
                        <button class="compare-btn" onclick="closeDrillDown(); loadComparison(${row.old_los_id}, ${row.new_los_id})">
                            Compare
                        </button>
                    </td>
                `;
                tbody.appendChild(tr);
            });
        }

        function closeDrillDown() {
            document.getElementById('drillDownModal').style.display = 'none';
        }

        // Close modal when clicking outside
        window.onclick = function(event) {
            const modal = document.getElementById('drillDownModal');
            if (event.target === modal) {
                closeDrillDown();
            }
        }
    </script>
</body>
</html>
"""

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    print("\nStarting server at http://localhost:5002")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    app.run(debug=True, host='0.0.0.0', port=5002)