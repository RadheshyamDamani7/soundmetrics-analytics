"""
fix_csvs.py — Run this ONCE from your soundmetrics/ folder
It removes the mystery 'nan' column from the 3 problematic CSVs
then re-saves them cleanly.

Run:  python fix_csvs.py
"""
import pandas as pd
import os

CLEAN_DIR = "data/cleaned"

FILES = [
    "subscriptions_clean.csv",
    "payments_clean.csv",
    "support_tickets_clean.csv",
]

print("=" * 50)
print("  SoundMetrics — CSV Column Fixer")
print("=" * 50)

for fname in FILES:
    path = os.path.join(CLEAN_DIR, fname)

    if not os.path.exists(path):
        print(f"  ⚠️  Not found: {path}")
        continue

    # Load the file
    df = pd.read_csv(path, low_memory=False)

    print(f"\n  {fname}")
    print(f"  Columns before: {list(df.columns)}")

    # Drop ANY column whose name is:
    #   - literally 'nan'
    #   - starts with 'Unnamed'
    #   - is an empty string
    bad_cols = [
        c for c in df.columns
        if str(c).strip().lower() == 'nan'
        or str(c).strip() == ''
        or str(c).startswith('Unnamed')
    ]

    if bad_cols:
        df = df.drop(columns=bad_cols)
        print(f"  Dropped columns : {bad_cols}")
    else:
        print(f"  No bad columns found!")

    print(f"  Columns after  : {list(df.columns)}")

    # Re-save — index=False ensures no extra index column is added
    df.to_csv(path, index=False)
    print(f"  ✅ Re-saved cleanly ({len(df):,} rows)")

print("\n" + "=" * 50)
print("  All files fixed! Now run:")
print("  cd sql")
print("  python 02_load_data_fixed.py")
print("=" * 50)
