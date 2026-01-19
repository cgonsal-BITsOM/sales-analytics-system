
"""
utils/api_handler.py — Part 3: API Integration with DummyJSON (v3.1.1)

Provides:
  - fetch_all_products(limit=100)
  - create_product_mapping(api_products)
  - enrich_sales_data(transactions, product_mapping)
  - save_enriched_data(enriched_transactions, filename='data/enriched_sales_data.txt')

Notes:
  * Explicit "
" for all file writes
  * LF newlines; no leading indentation before docstring
  * Graceful error handling for HTTP and file I/O
"""
from __future__ import annotations
from typing import List, Dict, Any
import os
import re

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

BASE_URL = 'https://dummyjson.com/products'


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# ------------------------------
# Task 3.1 — Fetch Product Details
# ------------------------------

def fetch_all_products(limit: int = 100) -> List[Dict[str, Any]]:
    """Fetches up to `limit` products from DummyJSON. Returns [] on failure."""
    if requests is None:
        print('[api] requests package not available; returning empty list')
        return []
    try:
        resp = requests.get(BASE_URL, params={'limit': int(limit)}, timeout=15)
        resp.raise_for_status()
        data = resp.json() or {}
        prods = data.get('products', []) or []
        out: List[Dict[str, Any]] = []
        for p in prods:
            out.append({
                'id': p.get('id'),
                'title': p.get('title'),
                'category': p.get('category'),
                'brand': p.get('brand'),
                'price': p.get('price'),
                'rating': p.get('rating'),
            })
        print(f"[api] Fetched {len(out)} products successfully")
        return out
    except Exception as e:
        print(f"[api] Fetch failed: {e}; returning empty list")
        return []


def create_product_mapping(api_products: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Returns {id: {'title','category','brand','rating'}} mapping."""
    mapping: Dict[int, Dict[str, Any]] = {}
    for p in api_products:
        pid = p.get('id')
        if isinstance(pid, int):
            mapping[pid] = {
                'title': p.get('title'),
                'category': p.get('category'),
                'brand': p.get('brand'),
                'rating': p.get('rating'),
            }
    print(f"[api] Created product mapping for {len(mapping)} items")
    return mapping


# ------------------------------
# Task 3.2 — Enrich Sales Data and Save
# ------------------------------

def _extract_numeric_product_id(product_id: str) -> int | None:
    digits = re.sub(r'\D+', '', str(product_id))
    return int(digits) if digits else None


def save_enriched_data(enriched_transactions: List[Dict[str, Any]], filename: str = 'data/enriched_sales_data.txt') -> None:
    """Saves enriched transactions to a pipe-delimited file with API fields."""
    header = [
        'TransactionID','Date','ProductID','ProductName','Quantity','UnitPrice','CustomerID','Region',
        'API_Category','API_Brand','API_Rating','API_Match'
    ]
    _ensure_dir(os.path.dirname(filename) or '.')
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        f.write('|'.join(header) + '')
        for t in enriched_transactions:
            qv = t.get('Quantity', '')
            pv = t.get('UnitPrice', '')
            q_str = '' if str(qv).strip()=='' else str(int(qv))
            p_str = '' if str(pv).strip()=='' else str(float(pv))
            row = [
                str(t.get('TransactionID','')),
                str(t.get('Date','')),
                str(t.get('ProductID','')),
                str(t.get('ProductName','')),
                q_str,
                p_str,
                str(t.get('CustomerID','')),
                str(t.get('Region','')),
                '' if t.get('API_Category') is None else str(t.get('API_Category')),
                '' if t.get('API_Brand') is None else str(t.get('API_Brand')),
                '' if t.get('API_Rating') is None else str(t.get('API_Rating')),
                str(bool(t.get('API_Match', False)))
            ]
            f.write('|'.join(row) + '')
    print(f"[api] Enriched data saved to {filename}")


def enrich_sales_data(transactions: List[Dict[str, Any]], product_mapping: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enriches transactions with API fields and persists to data/enriched_sales_data.txt."""
    enriched: List[Dict[str, Any]] = []
    for t in transactions:
        rec = dict(t)
        num_id = _extract_numeric_product_id(rec.get('ProductID',''))
        if num_id is not None and num_id in product_mapping:
            info = product_mapping[num_id]
            rec['API_Category'] = info.get('category')
            rec['API_Brand'] = info.get('brand')
            rec['API_Rating'] = info.get('rating')
            rec['API_Match'] = True
        else:
            rec['API_Category'] = None
            rec['API_Brand'] = None
            rec['API_Rating'] = None
            rec['API_Match'] = False
        enriched.append(rec)
    try:
        save_enriched_data(enriched, filename='data/enriched_sales_data.txt')
    except Exception as e:
        print(f"[api] Warning: failed to save enriched data: {e}")
    print(f"[api] Enriched {len(enriched)} transactions")
    return enriched
