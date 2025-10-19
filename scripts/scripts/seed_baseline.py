# scripts/seed_baseline.py
import sys, os, re, pandas as pd

if len(sys.argv) < 3:
    print("Usage: python scripts/seed_baseline.py <path_to_seed_file> <YYYY-MM-DD>")
    sys.exit(1)

src_path = sys.argv[1]
date_str = sys.argv[2]  # e.g., 2025-10-18
out_path = os.path.join(os.path.dirname(__file__), "..", "data", f"{date_str}.csv")

# Try CSV first
try:
    df = pd.read_csv(src_path)
    # if it already has headers, try to map them
    rename = {}
    for c in df.columns:
        l = c.strip().lower()
        if l in ("rank", "#", "no", "number", "id"): rename[c] = "Rank"
        elif "address" in l: rename[c] = "Address"
        elif any(k in l for k in ("balance", "amount", "bc2")): rename[c] = "Balance_BC2"
    df = df.rename(columns=rename)
    if not set(["Rank","Address","Balance_BC2"]).issubset(df.columns):
        raise ValueError("CSV header not recognized, will try whitespace mode.")
except Exception:
    df = None

# Fallback: whitespace-delimited (rank address balance) per line
if df is None or not set(["Rank","Address","Balance_BC2"]).issubset(df.columns):
    with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()
    # robust line parser (accepts bech32 + legacy addrs, commas in numbers)
    addr_pat = r'(?:bc1[0-9a-z]{20,}|[13][A-Za-z0-9]{20,})'
    row_pat = re.compile(rf'^\s*(\d+)\s+({addr_pat})\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s*$', re.I|re.M)
    rows = []
    for m in row_pat.finditer(text):
        rank = int(m.group(1))
        addr = m.group(2)
        bal  = float(m.group(3).replace(",", ""))
        rows.append((rank, addr, bal))
    if not rows:
        print("Could not parse any rows. Is the file really raw text with `rank address balance` per line?")
        sys.exit(2)
    df = pd.DataFrame(rows, columns=["Rank","Address","Balance_BC2"])

# Clean + sort + save
df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce").astype("Int64")
df["Balance_BC2"] = (
    df["Balance_BC2"].astype(str).str.replace(",", "", regex=False).str.strip()
)
df["Balance_BC2"] = pd.to_numeric(df["Balance_BC2"], errors="coerce")
df = df.dropna(subset=["Rank","Address","Balance_BC2"]).copy()
df["Rank"] = df["Rank"].astype(int)
df = df.sort_values("Rank")
os.makedirs(os.path.join(os.path.dirname(__file__), "..", "data"), exist_ok=True)
df.to_csv(out_path, index=False)
print(f"[seed] Wrote baseline: {out_path}  | rows={len(df)}")
