
"""
utils/file_handler.py — hardened (v1.3.1)
- Fixes unterminated string literal by using s = line.rstrip('
')
- Consolidates Tasks 1.1, 1.2, 1.3
"""
from __future__ import annotations
from typing import List, Dict, Tuple, Optional
import os

EXPECTED_HEADERS = [
    'TransactionID', 'Date', 'ProductID', 'ProductName',
    'Quantity', 'UnitPrice', 'CustomerID', 'Region'
]
ENCODING_CANDIDATES = ['utf-8', 'latin-1', 'cp1252']
NUMERIC_STRIPPERS = [',', '$', '€', '£', ' ']

def _looks_like_header(line: str) -> bool:
    s = line.strip().replace(' ', '')
    expected = '|'.join(EXPECTED_HEADERS)
    if s == expected:
        return True
    lowered = line.lower()
    hits = sum(1 for t in EXPECTED_HEADERS if t.lower() in lowered)
    return hits >= 4

def _sanitize_number(value: str) -> str:
    if value is None:
        return ''
    v = value.strip()
    for ch in NUMERIC_STRIPPERS:
        v = v.replace(ch, '')
    return v

def _clean_product_name(name: str) -> str:
    if name is None:
        return ''
    cleaned = name.replace(',', ' ').strip()
    while '  ' in cleaned:
        cleaned = cleaned.replace('  ', ' ')
    return cleaned

def read_sales_data(filename: str) -> List[str]:
    """Reads sales data with encoding handling, skips header/empties, returns raw lines."""
    if not os.path.exists(filename):
        raise FileNotFoundError(
            f"Sales data file not found at: {filename}. "
            "Ensure the path is correct (e.g., 'data/sales_data.txt')."
        )

    last_error: Exception | None = None
    raw_lines: List[str] = []
    for enc in ENCODING_CANDIDATES:
        try:
            with open(filename, 'r', encoding=enc, errors='strict') as f:
                raw_lines = f.readlines()
            break
        except UnicodeDecodeError as e:
            last_error = e
            continue
    else:
        try:
            with open(filename, 'r', encoding='latin-1', errors='replace') as f:
                raw_lines = f.readlines()
        except Exception as e:
            raise (last_error or e)

    cleaned: List[str] = []

    if raw_lines:
        raw_lines[0] = raw_lines[0].lstrip('﻿')

    idx = 0
    while idx < len(raw_lines) and not raw_lines[idx].strip():
        idx += 1

    if idx < len(raw_lines) and _looks_like_header(raw_lines[idx]):
        idx += 1

    for line in raw_lines[idx:]:
        s = line.rstrip('')
        if s.strip():
            cleaned.append(s)
    return cleaned

def parse_transactions(raw_lines: List[str]) -> List[Dict[str, object]]:
    cleaned_rows: List[Dict[str, object]] = []
    for line in raw_lines:
        if not line or '|' not in line:
            continue
        parts = [p.strip() for p in line.split('|')]
        if len(parts) != len(EXPECTED_HEADERS):
            continue
        row = {EXPECTED_HEADERS[i]: parts[i] for i in range(len(EXPECTED_HEADERS))}
        row['ProductName'] = _clean_product_name(row.get('ProductName', ''))
        q_raw = _sanitize_number(row.get('Quantity', '')) or '0'
        p_raw = _sanitize_number(row.get('UnitPrice', '')) or '0'
        try:
            qty = int(float(q_raw))
            price = float(p_raw)
        except Exception:
            continue
        row['Quantity'] = qty
        row['UnitPrice'] = price
        cleaned_rows.append(row)
    return cleaned_rows

def validate_and_filter(
    transactions: List[Dict[str, object]],
    region: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
) -> Tuple[List[Dict[str, object]], int, Dict[str, int]]:
    total_input = len(transactions)

    regions = sorted({str(t.get('Region','')).strip() for t in transactions if str(t.get('Region','')).strip()})
    amounts = []
    for t in transactions:
        try:
            amounts.append(float(t.get('Quantity',0))*float(t.get('UnitPrice',0.0)))
        except Exception:
            continue
    min_amt = min(amounts) if amounts else 0.0
    max_amt = max(amounts) if amounts else 0.0

    print("Available regions:", ", ".join(regions) if regions else "<none>")
    print(f"Transaction amount range (min → max): {min_amt:.2f} → {max_amt:.2f}")

    required = set(EXPECTED_HEADERS)
    valid_records: List[Dict[str, object]] = []
    invalid_count = 0

    for t in transactions:
        if not required.issubset(t.keys()):
            invalid_count += 1; continue
        if any(str(t.get(k,'')).strip()=='' for k in EXPECTED_HEADERS):
            invalid_count += 1; continue
        try:
            qty = int(t['Quantity']); price = float(t['UnitPrice'])
        except Exception:
            invalid_count += 1; continue
        if qty <= 0 or price <= 0:
            invalid_count += 1; continue
        if not str(t['TransactionID']).startswith('T'):
            invalid_count += 1; continue
        if not str(t['ProductID']).startswith('P'):
            invalid_count += 1; continue
        if not str(t['CustomerID']).startswith('C'):
            invalid_count += 1; continue
        r = dict(t)
        r['Quantity']=qty; r['UnitPrice']=price; r['_amount']=qty*price
        valid_records.append(r)

    before_region = len(valid_records)
    if region:
        valid_records = [r for r in valid_records if str(r.get('Region','')).strip()==region]
    filtered_by_region = before_region - len(valid_records)
    if region:
        print(f"After region filter ({region}): {len(valid_records)} records (filtered {filtered_by_region})")

    before_amount = len(valid_records)
    if min_amount is not None:
        valid_records = [r for r in valid_records if r['_amount'] >= float(min_amount)]
    if max_amount is not None:
        valid_records = [r for r in valid_records if r['_amount'] <= float(max_amount)]
    filtered_by_amount = before_amount - len(valid_records)
    if min_amount is not None or max_amount is not None:
        print(f"After amount filter (min={min_amount}, max={max_amount}): {len(valid_records)} records (filtered {filtered_by_amount})")

    for r in valid_records:
        r.pop('_amount', None)

    summary = {
        'total_input': total_input,
        'invalid': invalid_count,
        'filtered_by_region': filtered_by_region,
        'filtered_by_amount': filtered_by_amount,
        'final_count': len(valid_records)
    }
    return valid_records, invalid_count, summary
