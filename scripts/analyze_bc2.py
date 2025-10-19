import os, json, datetime as dt
import pandas as pd
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
HISTORY_PATH = os.path.join(DATA_DIR, "history.json")

def load_snapshot(d):
    path = os.path.join(DATA_DIR, f"{d}.csv")
    return pd.read_csv(path) if os.path.exists(path) else None

def find_previous_date(days_back):
    # returns ISO date string if file exists
    target = (dt.datetime.utcnow().date() - dt.timedelta(days=days_back)).isoformat()
    # walk backwards until a file is found (up to 7 extra days buffer)
    for extra in range(0, 8):
        cand = (dt.datetime.fromisoformat(target) - dt.timedelta(days=extra)).date().isoformat()
        if os.path.exists(os.path.join(DATA_DIR, f"{cand}.csv")):
            return cand
    return None

def diff_frames(curr: pd.DataFrame, prev: pd.DataFrame):
    if prev is None:
        return {
            "new_wallets": [],
            "grown": [],
            "dumped": [],
            "biggest_gainer": None,
            "biggest_loser": None,
            "top100_net_change": None,
        }
    # align on address
    c = curr.set_index("Address")["Balance_BC2"]
    p = prev.set_index("Address")["Balance_BC2"]

    # new wallets
    new_wallets = list((set(c.index) - set(p.index)))

    # changes for common wallets
    common = c.index.intersection(p.index)
    delta = (c.loc[common] - p.loc[common]).sort_values(ascending=False)

    grown = delta[delta > 0].reset_index().rename(columns={0:"Delta_BC2", "index":"Address"})
    dumped = (-delta[delta < 0]).reset_index().rename(columns={0:"Delta_BC2", "index":"Address"})  # positive numbers

    # biggest absolute movers
    biggest_gainer = None if grown.empty else grown.iloc[0].to_dict()
    biggest_loser  = None if dumped.empty else dumped.iloc[0].to_dict()

    # top100 net change
    top100_curr = curr.nsmallest(100, columns=["Rank"])  # Rank 1..100
    # map those addresses to prev balances
    top100_addrs = top100_curr["Address"].tolist()
    top100_prev_bal = p.reindex(top100_addrs).fillna(0.0)
    top100_curr_bal = c.reindex(top100_addrs).fillna(0.0)
    top100_net = float(top100_curr_bal.sum() - top100_prev_bal.sum())

    return {
        "new_wallets": new_wallets,
        "grown": grown.head(50).to_dict(orient="records"),
        "dumped": dumped.head(50).to_dict(orient="records"),
        "biggest_gainer": biggest_gainer,
        "biggest_loser": biggest_loser,
        "top100_net_change": top100_net,
    }

def main():
    today = dt.datetime.utcnow().date().isoformat()
    curr = load_snapshot(today)
    if curr is None:
        raise SystemExit("No snapshot for today; run scraper first.")

    one_day = find_previous_date(1)
    one_week = find_previous_date(7)
    one_month = find_previous_date(30)

    results = {"as_of": today, "comparisons": {}}
    for label, prev_date in [("day", one_day), ("week", one_week), ("month", one_month)]:
        prev_df = load_snapshot(prev_date) if prev_date else None
        results["comparisons"][label] = diff_frames(curr, prev_df)
        results["comparisons"][label]["vs_date"] = prev_date

    # save history.json (append/replace today)
    history = {}
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    history[today] = results
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)

if __name__ == "__main__":
    main()
