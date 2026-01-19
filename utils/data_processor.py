
"""
utils/data_processor.py — Consolidated (v2.2)
Part 2: Data Processing (Lists, Dictionaries & Functions)
Includes:
  • calculate_total_revenue
  • region_wise_sales
  • top_selling_products
  • customer_analysis
  • daily_sales_trend
  • find_peak_sales_day
  • low_performing_products
Assumes transactions are dicts with keys:
['TransactionID','Date','ProductID','ProductName','Quantity','UnitPrice','CustomerID','Region']
"""
from __future__ import annotations
from typing import List, Dict, Tuple, Iterable

# Helper to compute amount safely

def _amount(txn: Dict[str, object]) -> float:
    try:
        qty = int(txn.get('Quantity', 0))
        price = float(txn.get('UnitPrice', 0.0))
        return float(qty) * price
    except Exception:
        return 0.0

# ------------------------------
# Task 2.1 — Sales Summary Calculator
# ------------------------------

def calculate_total_revenue(transactions: Iterable[Dict[str, object]]) -> float:
    total = 0.0
    for t in transactions:
        total += _amount(t)
    return float(total)

# ------------------------------
# Task 2.2 — Region-wise Sales Analysis
# ------------------------------

def region_wise_sales(transactions: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, float]]:
    totals: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    for t in transactions:
        r = str(t.get('Region','')).strip() or 'Unknown'
        a = _amount(t)
        totals[r] = totals.get(r,0.0)+a
        counts[r] = counts.get(r,0)+1
    grand = sum(totals.values())
    rows: List[Tuple[str, Dict[str, float]]] = []
    for r, ts in totals.items():
        pct = (ts/grand*100.0) if grand>0 else 0.0
        rows.append((r, {
            'total_sales': float(ts),
            'transaction_count': int(counts.get(r,0)),
            'percentage': round(pct,2),
        }))
    rows.sort(key=lambda kv: kv[1]['total_sales'], reverse=True)
    ordered: Dict[str, Dict[str, float]] = {}
    for r, stats in rows:
        ordered[r]=stats
    return ordered

# ------------------------------
# Task 2.3 — Top Selling Products
# ------------------------------

def top_selling_products(transactions: Iterable[Dict[str, object]], n: int = 5) -> List[Tuple[str,int,float]]:
    qty: Dict[str,int] = {}
    rev: Dict[str,float] = {}
    for t in transactions:
        name = str(t.get('ProductName','')).strip() or 'Unknown'
        try:
            q = int(t.get('Quantity',0))
        except Exception:
            q = 0
        r = _amount(t)
        qty[name] = qty.get(name,0)+q
        rev[name] = rev.get(name,0.0)+r
    rows = [(name, qty[name], float(rev.get(name,0.0))) for name in qty.keys()]
    rows.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return rows[:max(int(n),0)]

# ------------------------------
# Customer Purchase Analysis
# ------------------------------

def customer_analysis(transactions: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    spent: Dict[str,float] = {}
    counts: Dict[str,int] = {}
    prods: Dict[str,set] = {}
    for t in transactions:
        cid = str(t.get('CustomerID','')).strip() or 'Unknown'
        spent[cid] = spent.get(cid,0.0) + _amount(t)
        counts[cid] = counts.get(cid,0)+1
        pname = str(t.get('ProductName','')).strip() or 'Unknown'
        s = prods.get(cid)
        if s is None:
            s = set()
            prods[cid] = s
        s.add(pname)
    items: List[Tuple[str, Dict[str, object]]] = []
    for cid in spent.keys():
        total = float(spent[cid])
        cnt = int(counts.get(cid,0))
        aov = round(total/cnt,2) if cnt>0 else 0.0
        items.append((cid, {
            'total_spent': total,
            'purchase_count': cnt,
            'avg_order_value': aov,
            'products_bought': sorted(list(prods.get(cid,set())))
        }))
    items.sort(key=lambda kv: kv[1]['total_spent'], reverse=True)
    ordered: Dict[str, Dict[str, object]] = {}
    for cid, stats in items:
        ordered[cid]=stats
    return ordered

# ------------------------------
# Task 2.2 — Date-based Analysis
# ------------------------------

def daily_sales_trend(transactions: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    daily: Dict[str, Dict[str, object]] = {}
    for t in transactions:
        date = str(t.get('Date','')).strip()
        if not date:
            continue
        amt = _amount(t)
        cust = str(t.get('CustomerID','')).strip()
        if date not in daily:
            daily[date] = {'revenue':0.0,'transaction_count':0,'_custs': set()}
        daily[date]['revenue'] = float(daily[date]['revenue']) + amt
        daily[date]['transaction_count'] = int(daily[date]['transaction_count']) + 1
        if cust:
            daily[date]['_custs'].add(cust)
    ordered: Dict[str, Dict[str, object]] = {}
    for d in sorted(daily.keys()):
        ordered[d] = {
            'revenue': round(float(daily[d]['revenue']),2),
            'transaction_count': int(daily[d]['transaction_count']),
            'unique_customers': len(daily[d]['_custs'])
        }
    return ordered


def find_peak_sales_day(transactions: Iterable[Dict[str, object]]):
    trend = daily_sales_trend(transactions)
    if not trend:
        return None
    peak_date = max(trend.keys(), key=lambda dd: float(trend[dd]['revenue']))
    rec = trend[peak_date]
    return (peak_date, rec['revenue'], rec['transaction_count'])

# ------------------------------
# Task 2.3 — Product Performance: Low Performing Products
# ------------------------------

def low_performing_products(transactions: Iterable[Dict[str, object]], threshold: int = 10):
    agg_qty: Dict[str,int] = {}
    agg_rev: Dict[str,float] = {}
    for t in transactions:
        name = str(t.get('ProductName','')).strip() or 'Unknown'
        try:
            q = int(t.get('Quantity',0))
        except Exception:
            q = 0
        r = _amount(t)
        agg_qty[name] = agg_qty.get(name,0)+q
        agg_rev[name] = agg_rev.get(name,0.0)+r
    rows = [(name, agg_qty[name], float(agg_rev.get(name,0.0))) for name in agg_qty.keys() if agg_qty[name] < int(threshold)]
    rows.sort(key=lambda x: (x[1], x[2], x[0]))
    return rows
