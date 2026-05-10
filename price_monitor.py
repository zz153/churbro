#!/usr/bin/env python3
"""
ChurBro Price Monitor
- Compares today's master CSV against the most recent previous day's master CSV
- Detects price increases, decreases, new products, and dropped products
- Saves a price_changes.json to api/ for the web app to use
- Maintains a rolling price_history.json with up to 30 days of history per product
"""

import pandas as pd
import json
import glob
import os
import re
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
API_DIR = "api"
HISTORY_FILE = os.path.join(API_DIR, "price_history.json")
CHANGES_FILE = os.path.join(API_DIR, "price_changes.json")
MAX_HISTORY_DAYS = 30
MIN_PRICE_CHANGE = 0.01   # ignore rounding noise below 1 cent


# ── Helpers ──────────────────────────────────────────────────────────────────

def find_master_csvs():
    """Return all churbro_master_*.csv files sorted newest-first."""
    files = glob.glob("churbro_data_*/churbro_master_*.csv")
    files.sort(reverse=True)
    return files


def load_master(path):
    """Load a master CSV and return a clean DataFrame."""
    df = pd.read_csv(path)
    # Normalise name: strip newlines/whitespace
    df['name'] = df['name'].astype(str).str.replace(r'\n+', ' ', regex=True).str.strip()
    # Build a stable key: store + normalised name
    df['_key'] = df['store'].str.strip() + '||' + df['name'].str.lower()
    return df


def extract_date_from_path(path):
    """Pull the date string out of churbro_data_YYYY-MM-DD/..."""
    match = re.search(r'churbro_data_(\d{4}-\d{2}-\d{2})', path)
    return match.group(1) if match else "unknown"


# ── Core logic ────────────────────────────────────────────────────────────────

def detect_changes(today_df, prev_df):
    """Compare two master DataFrames and return lists of changes."""

    today_df  = today_df.drop_duplicates('_key')
    prev_df   = prev_df.drop_duplicates('_key')
    today_map = dict(zip(today_df['_key'], today_df['price']))
    prev_map  = dict(zip(prev_df['_key'],  prev_df['price']))

    today_info = today_df.drop_duplicates('_key').set_index('_key')[['store', 'name', 'brand', 'price',
                                              'price_per_kg', 'unit_type']].to_dict('index')
    prev_info  = prev_df.drop_duplicates('_key').set_index('_key')[['store', 'name', 'brand', 'price',
                                             'price_per_kg', 'unit_type']].to_dict('index')

    price_drops      = []
    price_increases  = []
    new_products     = []
    dropped_products = []

    all_keys = set(today_map) | set(prev_map)

    for key in all_keys:
        in_today = key in today_map
        in_prev  = key in prev_map

        if in_today and in_prev:
            diff = today_map[key] - prev_map[key]
            if abs(diff) >= MIN_PRICE_CHANGE:
                info = today_info[key]
                entry = {
                    'store':        info['store'],
                    'name':         info['name'],
                    'brand':        info.get('brand', ''),
                    'old_price':    round(prev_map[key], 2),
                    'new_price':    round(today_map[key], 2),
                    'change':       round(diff, 2),
                    'change_pct':   round(diff / prev_map[key] * 100, 1),
                    'unit_type':    info.get('unit_type', ''),
                    'price_per_kg': info.get('price_per_kg', None),
                }
                if diff < 0:
                    price_drops.append(entry)
                else:
                    price_increases.append(entry)

        elif in_today and not in_prev:
            info = today_info[key]
            new_products.append({
                'store':        info['store'],
                'name':         info['name'],
                'brand':        info.get('brand', ''),
                'price':        round(today_map[key], 2),
                'unit_type':    info.get('unit_type', ''),
            })

        else:  # in prev but not today
            info = prev_info[key]
            dropped_products.append({
                'store': info['store'],
                'name':  info['name'],
                'brand': info.get('brand', ''),
                'price': round(prev_map[key], 2),
            })

    # Sort drops by biggest saving first
    price_drops.sort(key=lambda x: x['change'])
    price_increases.sort(key=lambda x: x['change'], reverse=True)

    return price_drops, price_increases, new_products, dropped_products


def update_history(today_df, today_date):
    """Append today's prices to the rolling history JSON."""

    # Load existing history
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
    else:
        history = {}

    cutoff = (datetime.strptime(today_date, '%Y-%m-%d') - timedelta(days=MAX_HISTORY_DAYS)).strftime('%Y-%m-%d')

    for _, row in today_df.iterrows():
        key = row['_key']
        if key not in history:
            history[key] = {
                'store':     row['store'],
                'name':      row['name'],
                'brand':     str(row.get('brand', '') or ''),
                'unit_type': str(row.get('unit_type', '') or ''),
                'prices':    []
            }

        # Append today's price point
        history[key]['prices'].append({
            'date':  today_date,
            'price': round(float(row['price']), 2),
        })

        # Trim old entries beyond MAX_HISTORY_DAYS
        history[key]['prices'] = [
            p for p in history[key]['prices'] if p['date'] >= cutoff
        ]

    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

    print(f"   📈 History updated: {len(history)} tracked products")
    return history


def save_changes_json(price_drops, price_increases, new_products, dropped_products, today_date, prev_date):
    """Write price_changes.json for the web app."""

    os.makedirs(API_DIR, exist_ok=True)

    output = {
        'generated_at': datetime.now().isoformat(),
        'today':        today_date,
        'compared_to':  prev_date,
        'summary': {
            'price_drops':     len(price_drops),
            'price_increases': len(price_increases),
            'new_products':    len(new_products),
            'dropped_products': len(dropped_products),
        },
        'price_drops':      price_drops[:50],       # top 50 drops
        'price_increases':  price_increases[:50],   # top 50 rises
        'new_products':     new_products[:50],
        'dropped_products': dropped_products[:50],
    }

    with open(CHANGES_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"   💾 Saved: {CHANGES_FILE}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 70)
    print("📊 CHURBRO PRICE MONITOR")
    print("=" * 70)

    masters = find_master_csvs()

    if len(masters) < 2:
        print("⚠️  Need at least 2 days of data to compare. Skipping.")
        return

    today_path = masters[0]
    prev_path  = masters[1]

    today_date = extract_date_from_path(today_path)
    prev_date  = extract_date_from_path(prev_path)

    print(f"📅 Today:    {today_date}  ({today_path})")
    print(f"📅 Previous: {prev_date}  ({prev_path})")
    print()

    today_df = load_master(today_path)
    prev_df  = load_master(prev_path)

    print(f"📦 Today's products:    {len(today_df)}")
    print(f"📦 Previous products:   {len(prev_df)}")
    print()

    # Detect changes
    drops, increases, new_prods, dropped = detect_changes(today_df, prev_df)

    print(f"✅ Price drops:      {len(drops)}")
    print(f"⬆️  Price increases:  {len(increases)}")
    print(f"🆕 New products:     {len(new_prods)}")
    print(f"❌ Dropped products: {len(dropped)}")
    print()

    # Print top drops
    if drops:
        print("🔥 TOP 10 PRICE DROPS:")
        for i, p in enumerate(drops[:10], 1):
            print(f"   {i:2}. [{p['store'].upper():8s}] {p['name'][:45]:45s}  "
                  f"${p['old_price']:.2f} → ${p['new_price']:.2f}  "
                  f"({p['change_pct']:+.1f}%)")
        print()

    # Print top increases
    if increases:
        print("📈 TOP 5 PRICE INCREASES:")
        for i, p in enumerate(increases[:5], 1):
            print(f"   {i:2}. [{p['store'].upper():8s}] {p['name'][:45]:45s}  "
                  f"${p['old_price']:.2f} → ${p['new_price']:.2f}  "
                  f"({p['change_pct']:+.1f}%)")
        print()

    # Update rolling history
    print("📈 Updating price history...")
    update_history(today_df, today_date)

    # Save changes JSON for web app
    print("💾 Saving price changes for web app...")
    save_changes_json(drops, increases, new_prods, dropped, today_date, prev_date)

    print()
    print("✅ Price monitoring complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
