#!/usr/bin/env python3
"""
PAK'nSAVE Data Cleanup Script - V3
Works with automated_scraper_PS_FIXED_V3.py
Removes products with price < $5
"""

import pandas as pd
from datetime import datetime
import sys
import glob

def clean_paknsave(input_file: str):
    """
    Clean PAK'nSAVE V3 data:
    1. Remove products with price < $5
    2. Add 'badge_type' and 'percent_off' columns
    3. Keep ALL other products (regardless of badge)
    
    Note: PAK'nSAVE doesn't show original prices, so percent_off is always 0
    """
    
    print("\n" + "="*70)
    print("🧹 PAK'nSAVE DATA CLEANUP (V3)")
    print("="*70)
    
    # Load data
    print(f"\n📂 Loading: {input_file}")
    df = pd.read_csv(input_file)
    initial_count = len(df)
    
    # Check if this is V3 data (has price_per_kg and unit_type columns)
    is_v3 = 'price_per_kg' in df.columns and 'unit_type' in df.columns
    print(f"   Data version: {'V3 (new)' if is_v3 else 'V2 (old)'}")
    
    print(f"\n📊 BEFORE CLEANUP:")
    print(f"   Total products: {initial_count}")
    print(f"   Price range: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
    if is_v3:
        print(f"   Unit types: {df['unit_type'].value_counts().to_dict()}")
    print(f"   Everyday Low: {df['is_everyday_low'].sum()}")
    print(f"   Extra Low: {df['is_extra_low'].sum()}")
    print(f"   Super Deal: {df['is_super_deal'].sum()}")
    total_badges = (df['is_everyday_low'] | df['is_extra_low'] | df['is_super_deal']).sum()
    print(f"   Total with badges: {total_badges}")
    
    # RULE 1: Remove cheap products (< $5)
    print(f"\n🔪 Removing products with price < $5...")
    df = df[df['price'] >= 5.0]
    final_count = len(df)
    removed = initial_count - final_count
    print(f"   ✂️  Removed: {removed} products")
    
    # RULE 2: Add badge type column
    print(f"\n🏷️  Adding badge_type column...")
    def get_badge_type(row):
        badges = []
        if row['is_everyday_low']:
            badges.append('EVERYDAY')
        if row['is_extra_low']:
            badges.append('EXTRA')
        if row['is_super_deal']:
            badges.append('SUPER')
        return ', '.join(badges) if badges else None  # ⚡ FIX: Return None instead of 'NONE'
    
    df['badge_type'] = df.apply(get_badge_type, axis=1)
    
    # RULE 3: Add percent_off column (always 0 for PAK'nSAVE)
    df['percent_off'] = 0.0
    
    # Summary statistics
    print(f"\n📊 AFTER CLEANUP:")
    print(f"   Total products: {final_count}")
    print(f"   Total removed: {removed} ({((removed) / initial_count * 100):.1f}%)")
    print(f"   Price range: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
    if is_v3:
        print(f"   Unit types: {df['unit_type'].value_counts().to_dict()}")
        has_per_kg = df['price_per_kg'].notna().sum()
        print(f"   Products with per kg price: {has_per_kg}")
    print(f"   Everyday Low: {df['is_everyday_low'].sum()}")
    print(f"   Extra Low: {df['is_extra_low'].sum()}")
    print(f"   Super Deal: {df['is_super_deal'].sum()}")
    print(f"   Products with badges: {df['badge_type'].notna().sum()}")  # ⚡ FIX: Check for notna() instead of != 'NONE'
    print(f"   Average price: ${df['price'].mean():.2f}")
    
    # Show top 10 cheapest products with badges (good deals!)
    with_badges = df[df['badge_type'].notna()]  # ⚡ FIX: Use notna() instead of != 'NONE'
    if len(with_badges) > 0:
        print(f"\n💰 TOP 10 CHEAPEST ITEMS WITH BADGES:")
        cheapest = with_badges.nsmallest(10, 'price')
        for i, row in enumerate(cheapest.itertuples(), 1):
            unit_info = f" ({row.unit_type})" if is_v3 else ""
            per_kg_info = f" [${row.price_per_kg:.2f}/kg]" if is_v3 and pd.notna(row.price_per_kg) else ""
            print(f"   {i:2d}. {row.name[:35]:35s} ${row.price:6.2f}{unit_info}{per_kg_info} [{row.badge_type}]")
    
    # Show sample products by badge type
    if final_count > 0:
        print(f"\n🏷️  SAMPLE PRODUCTS BY BADGE TYPE:")
        
        # Extra Low (most common)
        extra = df[df['is_extra_low'] == True].head(3)
        if len(extra) > 0:
            print(f"\n   🔴 EXTRA LOW ({df['is_extra_low'].sum()} total):")
            for i, row in enumerate(extra.itertuples(), 1):
                unit_info = f" ({row.unit_type})" if is_v3 else ""
                print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit_info}")
        
        # Everyday Low
        everyday = df[df['is_everyday_low'] == True].head(3)
        if len(everyday) > 0:
            print(f"\n   🟡 EVERYDAY LOW ({df['is_everyday_low'].sum()} total):")
            for i, row in enumerate(everyday.itertuples(), 1):
                unit_info = f" ({row.unit_type})" if is_v3 else ""
                print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit_info}")
        
        # Super Deal
        super_deals = df[df['is_super_deal'] == True].head(3)
        if len(super_deals) > 0:
            print(f"\n   🟢 SUPER DEAL ({df['is_super_deal'].sum()} total):")
            for i, row in enumerate(super_deals.itertuples(), 1):
                unit_info = f" ({row.unit_type})" if is_v3 else ""
                print(f"      {i}. {row.name[:45]:45s} ${row.price:6.2f}{unit_info}")
    
    # Save cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"paknsave_cleaned_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\n✅ SAVED: {output_file}")
    print(f"   Columns: {', '.join(df.columns)}")
    print(f"   Ready for ChurBro app!\n")
    
    return df, output_file

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Auto-detect latest PAK'nSAVE CSV file
        files = glob.glob('paknsave_deals_*.csv')
        files = [f for f in files if 'cleaned' not in f]
        
        if not files:
            print("❌ No PAK'nSAVE CSV file found!")
            print("Usage: python cleanup_paknsave.py <input_file.csv>")
            print("   Or: Run scraper first to generate data")
            sys.exit(1)
        
        # Use most recent file
        files.sort(reverse=True)
        input_file = files[0]
        print(f"📁 Auto-detected: {input_file}")
    
    try:
        clean_paknsave(input_file)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
