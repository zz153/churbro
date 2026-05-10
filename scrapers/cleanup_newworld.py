#!/usr/bin/env python3
"""
New World Data Cleanup Script - V3
Works with automated_scraper_NW_FIXED_V3.py
Removes products with price < $5
"""

import pandas as pd
from datetime import datetime
import sys
import glob

def clean_newworld(input_file: str):
    """
    Clean New World V3 data:
    1. Remove products with sale_price < $5
    2. Recalculate 'saving' and 'percent_off' columns
    3. Keep ALL other products (regardless of discount or badge)
    """
    
    print("\n" + "="*70)
    print("🧹 NEW WORLD DATA CLEANUP (V3)")
    print("="*70)
    
    # Load data
    print(f"\n📂 Loading: {input_file}")
    df = pd.read_csv(input_file)
    initial_count = len(df)
    
    # Check if this is V3 data (has price_per_kg column)
    is_v3 = 'price_per_kg' in df.columns
    print(f"   Data version: {'V3 (new)' if is_v3 else 'V2 (old)'}")
    
    print(f"\n📊 BEFORE CLEANUP:")
    print(f"   Total products: {initial_count}")
    print(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
    if is_v3:
        print(f"   Unit types: {df['unit_type'].value_counts().to_dict()}")
    print(f"   Club Deals: {df['is_club_deal'].sum()}")
    print(f"   Super Savers: {df['is_super_saver'].sum()}")
    
    # RULE 1: Remove cheap products (< $5)
    print(f"\n🔪 Removing products with sale_price < $5...")
    # Pre-calculate raw percent off to detect suspicious prices
    df['percent_off_raw'] = ((df['original_price'] - df['sale_price']) / df['original_price'].replace(0, 1) * 100)

    df = df[df['sale_price'] >= 5.0]
    final_count = len(df)
    removed = initial_count - final_count
    print(f"   ✂️  Removed: {removed} products")
    
    # RULE 2: Recalculate savings and percentages for ALL products
    print(f"\n💰 Recalculating savings...")
    # Fix: if sale_price is suspiciously low vs original (>80% off and sale_price < $10)
    # it's likely a per-unit reference price being misread as the sale price
    suspicious = (df['percent_off_raw'] > 80) & (df['sale_price'] < 10) & (df['original_price'] > 20)
    df.loc[suspicious, 'sale_price'] = df.loc[suspicious, 'original_price']

    df['saving'] = (df['original_price'] - df['sale_price']).round(2)
    df['percent_off'] = ((df['original_price'] - df['sale_price']) / df['original_price'] * 100).round(1)
    
    # Fix any NaN or negative values
    df['saving'] = df['saving'].fillna(0.0)
    df['percent_off'] = df['percent_off'].fillna(0.0)
    df.loc[df['saving'] < 0, 'saving'] = 0.0
    df.loc[df['percent_off'] < 0, 'percent_off'] = 0.0
    
    # Summary statistics
    print(f"\n📊 AFTER CLEANUP:")
    print(f"   Total products: {final_count}")
    print(f"   Total removed: {removed} ({((removed) / initial_count * 100):.1f}%)")
    print(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
    if is_v3:
        print(f"   Unit types: {df['unit_type'].value_counts().to_dict()}")
        has_per_kg = df['price_per_kg'].notna().sum()
        print(f"   Products with per kg price: {has_per_kg}")
    print(f"   Club Deals: {df['is_club_deal'].sum()}")
    print(f"   Super Savers: {df['is_super_saver'].sum()}")
    print(f"   Products with discount: {(df['saving'] > 0).sum()}")
    if (df['saving'] > 0).sum() > 0:
        print(f"   Average discount: ${df[df['saving'] > 0]['saving'].mean():.2f} ({df[df['saving'] > 0]['percent_off'].mean():.1f}% off)")
    
    # Show top 10 products with best discounts
    discounted = df[df['saving'] > 0]
    if len(discounted) > 0:
        print(f"\n🏆 TOP 10 DEALS (by % off):")
        top_deals = discounted.nlargest(10, 'percent_off')
        for i, row in enumerate(top_deals.itertuples(), 1):
            badge = "CLUB" if row.is_club_deal else ("SUPER" if row.is_super_saver else "")
            unit_info = f" ({row.unit_type})" if is_v3 else ""
            print(f"   {i:2d}. {row.name[:35]:35s} ${row.sale_price:6.2f} (was ${row.original_price:.2f}) -{row.percent_off:4.1f}%{unit_info} [{badge}]")
    else:
        print(f"\n⚠️  No products with discounts found - check if scraper is working correctly!")
    
    # Save cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"newworld_cleaned_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\n✅ SAVED: {output_file}")
    print(f"   Columns: {', '.join(df.columns)}")
    print(f"   Ready for ChurBro app!\n")
    
    return df, output_file

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Auto-detect latest New World CSV file
        files = glob.glob('newworld_specials_*.csv')
        files = [f for f in files if 'cleaned' not in f]
        
        if not files:
            print("❌ No New World CSV file found!")
            print("Usage: python cleanup_newworld.py <input_file.csv>")
            print("   Or: Run scraper first to generate data")
            sys.exit(1)
        
        # Use most recent file
        files.sort(reverse=True)
        input_file = files[0]
        print(f"📁 Auto-detected: {input_file}")
    
    try:
        clean_newworld(input_file)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
