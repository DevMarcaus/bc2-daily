import re, json, os, datetime as dt
import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = "https://bitcoinii.ddns.net/DailyReport.html"
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

def parse_page(html: str):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")

    # --- meta fields ---
    gen_on = re.search(r"Generated on:\s*(.+)", text)
    block_h = re.search(r"Current Block Height:\s*([0-9]+)", text)
    total_coins = re.search(r"Total Coins Mined:\s*([0-9,.\s]+)\s*BC2", text)

    generated_on = gen_on.group(1).strip() if gen_on else None
    block_height = int(block_h.group(1)) if block_h else None
    total_supply = float(total_coins.group(1).replace(",", "").strip()) if total_coins else None

    # --- robust row parse ---
    # Accepts:
    #  Rank (digits)
    #  Address (bc1... bech32 OR 1/3 legacy), no spaces
    #  Balance (int/float, optional commas)
    addr_pat = r'(?:bc1[0-9a-z]{20,}|[13][A-Za-z0-9]{20,})'   # loose but practical
    row_pat = re.compile(rf'^\s*(\d+)\s+({addr_pat})\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s*$', re.IGNORECASE | re.MULTILINE)

    rows = []
    # 1) try plain text
    for m in row_pat.finditer(text):
        rank = int(m.group(1))
        addr = m.group(2)
        bal = float(m.group(3).replace(",", ""))
        rows.append((rank, addr, bal))

    # 2) fallback: raw HTML
    if not rows:
        for m in row_pat.finditer(html):
            rank = int(m.group(1))
            addr = m.group(2)
            bal = float(m.group(3).replace(",", ""))
            rows.append((rank, addr, bal))

    df = pd.DataFrame(rows, columns=["Rank", "Address", "Balance_BC2"]).sort_values("Rank")
    meta = {
        "generated_on": generated_on,
        "block_height": block_height,
        "total_supply_bc2": total_supply,
        "source_url": URL,
    }
    return df, meta

def main():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BC2Scraper/1.0"}
    r = requests.get(URL, timeout=60, headers=headers)
    r.raise_for_status()

    df, meta = parse_page(r.text)

    # If nothing parsed, dump debug files to inspect
    if df.empty:
        debug_html = os.path.join(OUT_DIR, "debug_latest.html")
        debug_txt  = os.path.join(OUT_DIR, "debug_latest.txt")
        with open(debug_html, "w", encoding="utf-8") as f:
            f.write(r.text)
        with open(debug_txt, "w", encoding="utf-8") as f:
            f.write(BeautifulSoup(r.text, "html.parser").get_text("\n"))
        print(f"[scrape][warn] Parsed 0 rows. Wrote debug to:\n  {debug_html}\n  {debug_txt}")

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    out_csv = os.path.join(OUT_DIR, f"{today}.csv")

    df.to_csv(out_csv, index=False)
    df.to_csv(os.path.join(OUT_DIR, "latest.csv"), index=False)
    with open(os.path.join(OUT_DIR, "latest_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"[scrape] saved {len(df)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
