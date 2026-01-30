#!/usr/bin/env python3
"""
Mad Butcher Data Cleanup Script
Works with automated_scraper_MB.py
Removes products with price < $5 and invalid products
"""

import pandas as pd
from datetime import datetime
import sys
import glob

def clean_madbutcher(input_file: str):
    """
    Clean Mad Butcher data:
    1. Remove products with price < $5
    2. Remove invalid products (name too short, junk text)
    3. Add 'badge_type' column (SALE for products with savings)
    4. Calculate 'percent_off' from original_price and sale_price
    5. Rename 'sale_price' to 'price' for consistency
    """
    
    print("\n" + "="*70)
    print("🥩 MAD BUTCHER DATA CLEANUP")
    print("="*70)
    
    # Load data
    print(f"\n📂 Loading: {input_file}")
    df = pd.read_csv(input_file)
    initial_count = len(df)
    
    print(f"\n📊 BEFORE CLEANUP:")
    print(f"   Total products: {initial_count}")
    print(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
    print(f"   Unit types: {df['unit_type'].value_counts().to_dict()}")
    with_savings = df[df['saving'] > 0]
    print(f"   On sale: {len(with_savings)} products")
    if len(with_savings) > 0:
        print(f"   Average saving: ${with_savings['saving'].mean():.2f}")
    
    # RULE 1: Remove invalid products
    print(f"\n🔍 Checking for invalid products...")
    
    # Remove very short names (likely junk)
    before = len(df)
    df = df[df['name'].str.len() >= 10]
    removed_short = before - len(df)
    if removed_short > 0:
        print(f"   ✂️  Removed {removed_short} products with names < 10 chars")
    
    # Remove common junk text
    junk_patterns = ['specials', 'christmas', 'hours', 'closed', 'contact']
    for pattern in junk_patterns:
        before = len(df)
        df = df[~df['name'].str.lower().str.contains(pattern, na=False)]
        removed = before - len(df)
        if removed > 0:
            print(f"   ✂️  Removed {removed} products containing '{pattern}'")
    
    # RULE 2: Remove cheap products (< $5)
    print(f"\n🔪 Removing products with price < $5...")
    before = len(df)
    df = df[df['sale_price'] >= 0.5]
    removed_cheap = before - len(df)
    print(f"   ✂️  Removed: {removed_cheap} products")
    
    final_count = len(df)
    total_removed = initial_count - final_count
    
    # RULE 3: Add badge_type column
    print(f"\n🏷️  Adding badge_type column...")
    def get_badge_type(row):
        if row['saving'] > 0.5:  # At least 50 cents savings
            return 'SALE'
        return None
    
    df['badge_type'] = df.apply(get_badge_type, axis=1)
    
    # RULE 4: Calculate percent_off
    print(f"\n💯 Calculating percent_off...")
    def calc_percent_off(row):
        if row['original_price'] > 0 and row['saving'] > 0:
            return (row['saving'] / row['original_price']) * 100
        return 0.0
    
    df['percent_off'] = df.apply(calc_percent_off, axis=1)
    
    # RULE 5: Rename sale_price to price for consistency with other stores
    df = df.rename(columns={'sale_price': 'price'})
    
    # Summary statistics
    print(f"\n📊 AFTER CLEANUP:")
    print(f"   Total products: {final_count}")
    print(f"   Total removed: {total_removed} ({(total_removed / initial_count * 100):.1f}%)")
    print(f"   Price range: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
    print(f"   Unit types: {df['unit_type'].value_counts().to_dict()}")
    has_per_kg = df['price_per_kg'].notna().sum()
    print(f"   Products with per kg price: {has_per_kg}")
    on_sale = df[df['badge_type'] == 'SALE']
    print(f"   On SALE: {len(on_sale)} ({(len(on_sale) / final_count * 100):.1f}%)")
    if len(on_sale) > 0:
        print(f"   Average saving: ${on_sale['saving'].mean():.2f}")
        print(f"   Average discount: {on_sale['percent_off'].mean():.1f}%")
    print(f"   Average price: ${df['price'].mean():.2f}")
    
    # Show top 10 best deals (highest percent off)
    best_deals = df[df['percent_off'] > 0].nlargest(10, 'percent_off')
    if len(best_deals) > 0:
        print(f"\n💰 TOP 10 BEST DEALS (Highest % Off):")
        for i, row in enumerate(best_deals.itertuples(), 1):
            per_kg_info = f" [${row.price_per_kg:.2f}/kg]" if pd.notna(row.price_per_kg) else ""
            print(f"   {i:2d}. {row.name[:40]:40s} ${row.price:6.2f} (was ${row.original_price:.2f}) -{row.percent_off:4.1f}%{per_kg_info}")
    
    # Show cheapest products
    print(f"\n💵 TOP 10 CHEAPEST PRODUCTS:")
    cheapest = df.nsmallest(10, 'price')
    for i, row in enumerate(cheapest.itertuples(), 1):
        unit_info = f" ({row.unit_type})"
        per_kg_info = f" [${row.price_per_kg:.2f}/kg]" if pd.notna(row.price_per_kg) else ""
        badge = f" [SALE -{row.percent_off:.0f}%]" if row.percent_off > 0 else ""
        print(f"   {i:2d}. {row.name[:40]:40s} ${row.price:6.2f}{unit_info}{per_kg_info}{badge}")
    
    # Sample products by category
    print(f"\n🥩 SAMPLE PRODUCTS BY CATEGORY:")
    
    # Chicken
    chicken = df[df['name'].str.contains('chicken', case=False, na=False)].head(3)
    if len(chicken) > 0:
        print(f"\n   🐔 CHICKEN ({len(df[df['name'].str.contains('chicken', case=False, na=False)])} total):")
        for i, row in enumerate(chicken.itertuples(), 1):
            unit = f" ({row.unit_type})"
            per_kg = f" [${row.price_per_kg:.2f}/kg]" if pd.notna(row.price_per_kg) else ""
            print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit}{per_kg}")
    
    # Beef
    beef = df[df['name'].str.contains('beef', case=False, na=False)].head(3)
    if len(beef) > 0:
        print(f"\n   🥩 BEEF ({len(df[df['name'].str.contains('beef', case=False, na=False)])} total):")
        for i, row in enumerate(beef.itertuples(), 1):
            unit = f" ({row.unit_type})"
            per_kg = f" [${row.price_per_kg:.2f}/kg]" if pd.notna(row.price_per_kg) else ""
            print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit}{per_kg}")
    
    # Pork
    pork = df[df['name'].str.contains('pork', case=False, na=False)].head(3)
    if len(pork) > 0:
        print(f"\n   🐷 PORK ({len(df[df['name'].str.contains('pork', case=False, na=False)])} total):")
        for i, row in enumerate(pork.itertuples(), 1):
            unit = f" ({row.unit_type})"
            per_kg = f" [${row.price_per_kg:.2f}/kg]" if pd.notna(row.price_per_kg) else ""
            print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit}{per_kg}")
    
    # Lamb
    lamb = df[df['name'].str.contains('lamb', case=False, na=False)].head(3)
    if len(lamb) > 0:
        print(f"\n   🐑 LAMB ({len(df[df['name'].str.contains('lamb', case=False, na=False)])} total):")
        for i, row in enumerate(lamb.itertuples(), 1):
            unit = f" ({row.unit_type})"
            per_kg = f" [${row.price_per_kg:.2f}/kg]" if pd.notna(row.price_per_kg) else ""
            print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit}{per_kg}")
    
    # Save cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"madbutcher_cleaned_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\n✅ SAVED: {output_file}")
    print(f"   Columns: {', '.join(df.columns)}")
    print(f"   Ready for ChurBro app!\n")
    
    return df, output_file

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Auto-detect latest Mad Butcher CSV file
        files = glob.glob('madbutcher_products_*.csv')
        files = [f for f in files if 'cleaned' not in f]
        
        if not files:
            print("❌ No Mad Butcher CSV file found!")
            print("Usage: python cleanup_madbutcher.py <input_file.csv>")
            print("   Or: Run scraper first to generate data")
            sys.exit(1)
        
        # Use most recent file
        files.sort(reverse=True)
        input_file = files[0]
        print(f"🔍 Auto-detected: {input_file}")
    
    try:
        clean_madbutcher(input_file)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
