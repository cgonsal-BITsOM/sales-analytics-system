
# main.py — v0.5.1 
# Sales Analytics System - Main Execution Script
# Processes sales data, performs analysis, enriches with API data, and generates reports.
# Student Name: Charles Gonsalves
# Stundet ID: bitsom_ba_25071752
# Student Mail :cgonsal@gmail.com

import os
from typing import List, Dict, Any

from utils.file_handler import (
    read_sales_data,
    parse_transactions,
    validate_and_filter,
)
from utils.data_processor import (
    calculate_total_revenue,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
)
from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
)
from utils.reporting import generate_sales_report

# --- Local constants (kept simple & explicit) ---
OUTPUT_DIR = 'output'
DATA_DIR = 'data'
CURRENCY_SYMBOL = '₹'
API_LIMIT = 100
LOW_THRESHOLD = 10

# --- Small helpers ---
def _ensure_dirs() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

def _money(v: float) -> str:
    try:
        return f"{CURRENCY_SYMBOL}{v:,.2f}"
    except Exception:
        return f"{CURRENCY_SYMBOL}{v}"

def _available_regions_and_amount_range(parsed: List[Dict[str, Any]]):
    regions = sorted({str(t.get('Region','')).strip() for t in parsed if str(t.get('Region','')).strip()})
    amounts = []
    for t in parsed:
        try:
            amounts.append(float(t.get('Quantity', 0)) * float(t.get('UnitPrice', 0.0)))
        except Exception:
            pass
    min_amt = min(amounts) if amounts else 0.0
    max_amt = max(amounts) if amounts else 0.0
    return regions, min_amt, max_amt

def _banner():
    print("="*40)
    print("SALES ANALYTICS SYSTEM")
    print("="*40)

def _step(i: int, n: int, text: str):
    print(f"[{i}/{n}] {text}...")

def _ok(text: str):
    print(f"✓ {text}")


def main():
    """Main execution function (interactive, simplified)."""
    _ensure_dirs()
    _banner()

    try:
        # 1) Read ------------------------------------------------------------
        _step(1, 10, "Reading sales data")
        raw_lines = read_sales_data(os.path.join(DATA_DIR, 'sales_data.txt'))
        _ok(f"Successfully read {len(raw_lines)} transactions")

        # 2) Parse -----------------------------------------------------------
        _step(2, 10, "Parsing and cleaning data")
        parsed = parse_transactions(raw_lines)
        _ok(f"Parsed {len(parsed)} records")

        # 3) Filter options --------------------------------------------------
        _step(3, 10, "Filter Options Available")
        regions, min_amt, max_amt = _available_regions_and_amount_range(parsed)
        regions_line = ", ".join(regions) if regions else "<none>"
        print(f"Regions: {regions_line}")
        print(f"Amount Range: {_money(min_amt)} - {_money(max_amt)}")
        choice = input("Do you want to filter data? (y/n): ").strip().lower()
        sel_region = None; sel_min = None; sel_max = None
        if choice == 'y':
            r_in = input("Region (blank = none): ").strip()
            sel_region = r_in or None
            mi = input("Min amount (blank = none): ").strip()
            try: sel_min = float(mi) if mi else None
            except: sel_min = None
            ma = input("Max amount (blank = none): ").strip()
            try: sel_max = float(ma) if ma else None
            except: sel_max = None
        else:
            print("Proceeding without filters...")

        # 4) Validate --------------------------------------------------------
        _step(4, 10, "Validating transactions")
        valid, invalid_count, _ = validate_and_filter(parsed, region=sel_region, min_amount=sel_min, max_amount=sel_max)
        _ok(f"Valid: {len(valid)} | Invalid: {invalid_count}")

        # 5) Analyze (Part 2) -----------------------------------------------
        _step(5, 10, "Analyzing sales data")
        _ = calculate_total_revenue(valid)
        _ = region_wise_sales(valid)
        _ = top_selling_products(valid, n=5)
        _ = customer_analysis(valid)
        _ = daily_sales_trend(valid)
        _ = find_peak_sales_day(valid)
        _ = low_performing_products(valid, threshold=LOW_THRESHOLD)
        _ok("Analysis complete")

        # 6) Fetch API -------------------------------------------------------
        _step(6, 10, "Fetching product data from API")
        products = fetch_all_products(limit=API_LIMIT)
        _ok(f"Fetched {len(products)} products")

        # 7) Enrich ----------------------------------------------------------
        _step(7, 10, "Enriching sales data")
        mapping = create_product_mapping(products)
        enriched = enrich_sales_data(valid, mapping)  # also writes to data/enriched_sales_data.txt
        succ = sum(1 for t in enriched if bool(t.get('API_Match', False)))
        total = len(valid)
        rate = (succ/total*100.0) if total else 0.0
        _ok(f"Enriched {succ}/{total} transactions ({rate:.1f}%)")

        # 8) Save enriched ---------------------------------------------------
        _step(8, 10, "Saving enriched data")
        enriched_path = os.path.join(DATA_DIR, 'enriched_sales_data.txt')
        _ok(f"Saved to: {enriched_path}")

        # 9) Report ----------------------------------------------------------
        _step(9, 10, "Generating report")
        report_path = generate_sales_report(valid, enriched, output_file=os.path.join(OUTPUT_DIR, 'sales_report.txt'), currency_symbol=CURRENCY_SYMBOL, low_threshold=LOW_THRESHOLD)
        _ok(f"Report saved to: {report_path}")

        # 10) Done -----------------------------------------------------------
        _step(10, 10, "Process Complete!")
        print("="*40)

    except KeyboardInterrupt:
        print("Operation cancelled by user.")
        print("="*40)
    except Exception as e:
        print("An error occurred. Please check your input files and configuration.")
        print(f"Details: {e}")
        print("="*40)


if __name__ == '__main__':
    main()
