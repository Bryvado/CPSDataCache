#!/usr/bin/env python3
"""
update_history.py — build or extend data/state_series.json with state-level
CPS basic monthly aggregates (unemployment rate, LFPR, EPOP, labor force,
unweighted unemployed n) for all states, from the Census microdata API.

Run with no args. If data/state_series.json exists, it appends any months
released since the last one in the file. If it doesn't exist, it seeds the
full history from START (1994-01) to the latest available month.

Env: CENSUS_KEY (falls back to the key below).
"""
import json, os, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

KEY = os.environ.get("CENSUS_KEY", "b31cc9efde65bcbea4cf1b0a46f70fd08cdb557c")
MONTHS = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
START = (1994, 1)
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "state_series.json")
WORKERS = 8

def month_iter(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m > 12: m, y = 1, y + 1

def fetch_month(y, m, tries=4):
    url = (f"https://api.census.gov/data/{y}/cps/basic/{MONTHS[m-1]}"
           f"?get=PEMLR,PRTAGE,PWCMPWGT&for=state:*&key={KEY}")
    for a in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                raw = json.load(r)
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None          # month not released / doesn't exist
            time.sleep(2 * (a + 1))  # 503 etc
        except Exception:
            time.sleep(2 * (a + 1))
    else:
        raise RuntimeError(f"gave up on {y}-{m:02d}")
    ix = {c: i for i, c in enumerate(raw[0])}
    agg = {}
    for row in raw[1:]:
        try:
            w = float(row[ix["PWCMPWGT"]]); age = int(row[ix["PRTAGE"]])
            if w <= 0 or age < 16: continue
            st = str(row[ix["state"]]).zfill(2)
            a = agg.setdefault(st, [0.0, 0.0, 0.0, 0])
            mlr = row[ix["PEMLR"]]
            if not mlr: continue
            mlr = int(mlr)
        except (TypeError, ValueError, KeyError):
            continue
        a[2] += w
        if mlr in (1, 2): a[0] += w
        elif mlr in (3, 4):
            a[1] += w; a[3] += 1
    out = {}
    for st, (emp, unemp, pop, nU) in agg.items():
        lf = emp + unemp
        if lf <= 0 or pop <= 0: continue
        out[st] = [round(100*unemp/lf, 2), round(100*lf/pop, 2),
                   round(100*emp/pop, 2), int(round(lf)), nU]
    return out  # {fips: [ur, lfpr, epop, lf, nU]}

def latest_available(after):
    """Walk forward from `after` until a 404, return last existing month."""
    y, m = after
    last = None
    while True:
        m += 1
        if m > 12: m, y = 1, y + 1
        probe = (f"https://api.census.gov/data/{y}/cps/basic/{MONTHS[m-1]}"
                 f"?get=HRMONTH&for=state:11&key={KEY}")
        try:
            urllib.request.urlopen(probe, timeout=30).read(64)
            last = (y, m)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return last
            time.sleep(3)  # transient — retry same month
            m -= 1
            if m < 1: m, y = 12, y - 1
        except Exception:
            time.sleep(3); m -= 1
            if m < 1: m, y = 12, y - 1

def main():
    if os.path.exists(OUT):
        doc = json.load(open(OUT))
        ly, lm = map(int, doc["meta"]["end"].split("-"))
        have = (ly, lm)
        print(f"existing file ends {ly}-{lm:02d}")
    else:
        doc = {"meta": {"start": f"{START[0]}-{START[1]:02d}"}, "series": {}}
        have = (START[0], START[1] - 1)
        print("no existing file — seeding full history (this takes a while)")

    newest = latest_available(have)
    if newest is None:
        print("no new months available; nothing to do")
        return 0
    todo = list(month_iter((have[0], have[1] + 1) if have[1] < 12 else (have[0] + 1, 1), newest))
    # fix wrap for have[1]==12 handled by month_iter start above
    print(f"fetching {len(todo)} month(s) through {newest[0]}-{newest[1]:02d}")

    results = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_month, y, m): (y, m) for y, m in todo}
        done = 0
        for f in as_completed(futs):
            y, m = futs[f]
            results[(y, m)] = f.result()
            done += 1
            if done % 10 == 0 or done == len(todo):
                print(f"  {done}/{len(todo)}")

    for (y, m), agg in sorted(results.items()):
        if agg is None: continue
        k = f"{y}-{m:02d}"
        for st, vals in agg.items():
            doc["series"].setdefault(st, {})[k] = vals

    doc["meta"]["end"] = f"{newest[0]}-{newest[1]:02d}"
    doc["meta"]["built"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    doc["meta"]["note"] = ("NSA aggregates from CPS basic monthly microdata, "
                           "PWCMPWGT weight, civilian noninstitutional 16+. "
                           "Per state-month: [ur, lfpr, epop, labor_force, unweighted_unemp_n]")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(doc, open(OUT, "w"), separators=(",", ":"))
    print(f"wrote {OUT} ({os.path.getsize(OUT)//1024} KB), "
          f"{sum(len(v) for v in doc['series'].values())} state-months")
    return 0

if __name__ == "__main__":
    sys.exit(main())
