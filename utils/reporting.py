
"""
utils/reporting.py — Part 4: Report Generation (v4.0.0)

Task: Generate Comprehensive Text Report

Function:
    generate_sales_report(transactions, enriched_transactions, output_file='output/sales_report.txt')

- Writes a detailed, formatted text report with 8 sections in the specified order.
- Dependencies: utils.data_processor (Part 2 functions).
- Robust to missing enrichment fields (API_*), and ensures directories exist.
"""
from __future__ import annotations
from typing import List, Dict, Any, Iterable
import os
from datetime import datetime

# Import analytics utilities from Part 2
from utils.data_processor import (
    calculate_total_revenue,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
)

# -------------------------------------
# helpers
# -------------------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _fmt_money(value: float, currency_symbol: str = '₹') -> str:
    try:
        return f"{currency_symbol}{value:,.2f}"
    except Exception:
        return f"{currency_symbol}{value}"

def _safe_int(v) -> int:
    try:
        return int(v)
    except Exception:
        return 0

def _safe_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

# -------------------------------------
# main API
# -------------------------------------

def generate_sales_report(
    transactions: Iterable[Dict[str, Any]],
    enriched_transactions: Iterable[Dict[str, Any]] | None,
    output_file: str = 'output/sales_report.txt',
    currency_symbol: str = '₹',
    low_threshold: int = 10,
) -> str:
    """
    Generates a comprehensive formatted text report and writes it to output_file.

    Returns the absolute path of the written report.
    """
    # materialize inputs (iterables may be generators)
    txns = list(transactions or [])
    etxns = list(enriched_transactions or [])

    # Ensure output dir exists
    out_dir = os.path.dirname(output_file) or '.'
    _ensure_dir(out_dir)

    # 1) HEADER ---------------------------------------------------------------
    title_line = "SALES ANALYTICS REPORT"
    generated_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    records_processed = len(txns)

    # 2) OVERALL SUMMARY ------------------------------------------------------
    total_revenue = calculate_total_revenue(txns)
    total_transactions = len(txns)
    avg_order_value = (total_revenue / total_transactions) if total_transactions else 0.0

    # date range
    dates = [str(t.get('Date','')).strip() for t in txns if str(t.get('Date','')).strip()]
    date_min = min(dates) if dates else 'N/A'
    date_max = max(dates) if dates else 'N/A'

    # 3) REGION-WISE PERFORMANCE ---------------------------------------------
    region_stats = region_wise_sales(txns)  # already sorted by total_sales desc

    # 4) TOP 5 PRODUCTS -------------------------------------------------------
    top5_products = top_selling_products(txns, n=5)

    # 5) TOP 5 CUSTOMERS ------------------------------------------------------
    cust_stats = customer_analysis(txns)  # dict sorted by total_spent desc (insertion order)
    top5_customers = list(cust_stats.items())[:5]  # (CustomerID, stats dict)

    # 6) DAILY SALES TREND ----------------------------------------------------
    trend = daily_sales_trend(txns)  # chronologically sorted dict

    # 7) PRODUCT PERFORMANCE ANALYSIS ----------------------------------------
    peak = find_peak_sales_day(txns)  # (date, revenue, transaction_count) or None
    low_products = low_performing_products(txns, threshold=low_threshold)
    # Avg transaction value per region = region total / region count
    avg_per_region = []
    for r, stats in region_stats.items():
        cnt = _safe_int(stats.get('transaction_count', 0))
        tot = _safe_float(stats.get('total_sales', 0.0))
        avg = (tot / cnt) if cnt else 0.0
        avg_per_region.append((r, avg))

    # 8) API ENRICHMENT SUMMARY ---------------------------------------------
    # Count enriched successes and gather failures.
    api_total = len(etxns)
    api_success = 0
    not_enriched = []  # List[str] labels for failures
    for e in etxns:
        if bool(e.get('API_Match', False)):
            api_success += 1
        else:
            pid = str(e.get('ProductID',''))
            pname = str(e.get('ProductName',''))
            label = f"{pid}:{pname}".strip(':')
            if label:
                not_enriched.append(label)
    api_rate = (api_success / api_total * 100.0) if api_total else 0.0

    # ------------------------------------------------------------------------
    # Formatting sections
    # ------------------------------------------------------------------------
    lines: List[str] = []

    # Section header block
    sep = '=' * 44
    lines.append(sep)
    lines.append(title_line)
    lines.append(f"Generated: {generated_ts}")
    lines.append(f"Records Processed: {records_processed}")
    lines.append(sep)

    # OVERALL SUMMARY
    lines.append("OVERALL SUMMARY")
    lines.append('-' * 44)
    lines.append(f"Total Revenue: {_fmt_money(total_revenue, currency_symbol)}")
    lines.append(f"Total Transactions: {total_transactions}")
    lines.append(f"Average Order Value: {_fmt_money(avg_order_value, currency_symbol)}")
    lines.append(f"Date Range: {date_min} to {date_max}")

    # REGION-WISE PERFORMANCE table
    lines.append("REGION-WISE PERFORMANCE")
    lines.append('-' * 44)
    # header row
    lines.append(f"{'Region':<12}{'Sales':>15}  {'% of Total':>12}  {'Transactions':>13}")
    for region, stats in region_stats.items():
        sales = _safe_float(stats.get('total_sales', 0.0))
        pct = _safe_float(stats.get('percentage', 0.0))
        cnt = _safe_int(stats.get('transaction_count', 0))
        lines.append(f"{region:<12}{_fmt_money(sales, currency_symbol):>15}  {pct:>11.2f}%  {cnt:>13}")

    # TOP 5 PRODUCTS table
    lines.append("TOP 5 PRODUCTS")
    lines.append('-' * 44)
    lines.append(f"{'Rank':>4}  {'Product Name':<30}{'Qty':>6}  {'Revenue':>15}")
    for idx, (name, qty, rev) in enumerate(top5_products, start=1):
        lines.append(f"{idx:>4}  {name:<30}{qty:>6}  {_fmt_money(_safe_float(rev), currency_symbol):>15}")

    # TOP 5 CUSTOMERS table
    lines.append("TOP 5 CUSTOMERS")
    lines.append('-' * 44)
    lines.append(f"{'Rank':>4}  {'Customer ID':<12}{'Total Spent':>15}  {'Order Count':>12}")
    for idx, (cid, st) in enumerate(top5_customers, start=1):
        total_spent = _safe_float(st.get('total_spent', 0.0))
        orders = _safe_int(st.get('purchase_count', 0))
        lines.append(f"{idx:>4}  {cid:<12}{_fmt_money(total_spent, currency_symbol):>15}  {orders:>12}")

    # DAILY SALES TREND table
    lines.append("DAILY SALES TREND")
    lines.append('-' * 44)
    lines.append(f"{'Date':<12}{'Revenue':>15}  {'Trans':>7}  {'Unique Cust.':>14}")
    for d, rec in trend.items():
        rev = _safe_float(rec.get('revenue', 0.0))
        tc = _safe_int(rec.get('transaction_count', 0))
        uc = _safe_int(rec.get('unique_customers', 0))
        lines.append(f"{d:<12}{_fmt_money(rev, currency_symbol):>15}  {tc:>7}  {uc:>14}")

    # PRODUCT PERFORMANCE ANALYSIS
    lines.append("PRODUCT PERFORMANCE ANALYSIS")
    lines.append('-' * 44)
    # best selling day
    if peak:
        lines.append(f"Best Selling Day: {peak[0]}  (Revenue: {_fmt_money(_safe_float(peak[1]), currency_symbol)}, Transactions: {_safe_int(peak[2])})")
    else:
        lines.append("Best Selling Day: N/A")
    # low performing products
    if low_products:
        lines.append("Low Performing Products (< threshold)")
        lines.append(f"{'Product':<30}{'Qty':>6}  {'Revenue':>15}")
        for name, qty, rev in low_products:
            lines.append(f"{name:<30}{_safe_int(qty):>6}  {_fmt_money(_safe_float(rev), currency_symbol):>15}")
    else:
        lines.append("Low Performing Products: None")
    # average transaction value per region
    lines.append("Average Transaction Value per Region")
    lines.append(f"{'Region':<12}{'Avg Value':>15}")
    for r, avg in avg_per_region:
        lines.append(f"{r:<12}{_fmt_money(avg, currency_symbol):>15}")

    # API ENRICHMENT SUMMARY
    lines.append("API ENRICHMENT SUMMARY")
    lines.append('-' * 44)
    lines.append(f"Total products enriched: {api_success} of {api_total}")
    lines.append(f"Success rate: {api_rate:.2f}%")
    if not_enriched:
        lines.append("Products not enriched:")
        # Show unique, sorted, up to a reasonable limit
        uniq = sorted(set(not_enriched))
        for label in uniq:
            lines.append(f" - {label}")
    else:
        lines.append("All products enriched successfully (or enrichment not attempted).")

    # Write the file
    report_text = "\n".join(lines) + "\n"
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        f.write(report_text)

    return os.path.abspath(output_file)
