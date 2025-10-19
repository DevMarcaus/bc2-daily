import os, json
import pandas as pd
from jinja2 import Environment, FileSystemLoader

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA = os.path.join(ROOT, "data")
TEMPLATES = os.path.join(ROOT, "templates")
SITE = os.path.join(ROOT, "site")
os.makedirs(SITE, exist_ok=True)

def main():
    with open(os.path.join(DATA, "latest_meta.json")) as f:
        meta = json.load(f)
    with open(os.path.join(DATA, "history.json")) as f:
        history = json.load(f)

    today = sorted(history.keys())[-1]
    ctx = history[today]  # contains comparisons
    env = Environment(loader=FileSystemLoader(TEMPLATES))
    tpl = env.get_template("index.html.j2")
    html = tpl.render(meta=meta, **ctx)

    with open(os.path.join(SITE, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

if __name__ == "__main__":
    main()
