#!/usr/bin/env python3
"""
moonshot_scrape.py — pulls the daily MLB HR board from Moonshot
(https://my-new-sport-mlb.grok.me) and exports it as clean JSON + CSV.

The site server-renders its full data payload into an inline
<script id="$tsr-stream-barrier"> block using TanStack Router's
streaming format: a single big object literal with shared values
factored out as `$R[N]=value` backreferences (`$R[7]` reused later
as a bare `$R[7]`). This is NOT JSON — unquoted keys, `!0`/`!1` for
booleans, `undefined`/`void 0`, and the backreference indirection.
This script implements a small recursive-descent evaluator for that
format (eval_value/eval_object/eval_array/eval_ref below) and walks
the parsed router state to board.matches[1]['l'] (the route's
loader data), which holds {date, season, games, predictions, summary}.

Usage:
    python3 moonshot_scrape.py [YYYY-MM-DD]
        (defaults to today; date maps to the site's ?date= param)

Output (written next to this script):
    moonshot_<date>.json  — full parsed payload (date/games/predictions/summary)
    moonshot_<date>.csv   — one row per batter, ranked by pHr desc

No API key / auth needed — this is a plain GET, same as a browser.
Rerun daily (or wire into a cron/GitHub Actions job like the rest of
the pipeline scripts) since the board changes with lineups/odds/day.
"""
import sys, re, json, csv, urllib.request
from datetime import date as _date

BASE = "https://my-new-sport-mlb.grok.me/"


def fetch_html(date_str: str) -> str:
    url = f"{BASE}?date={date_str}" if date_str else BASE
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def extract_stream_script(html: str) -> str:
    start = html.find('id="$tsr-stream-barrier"')
    if start == -1:
        raise RuntimeError("stream barrier script not found — page structure may have changed")
    gt = html.find(">", start)
    end = html.find("</script>", gt)
    return html[gt + 1:end]


# ---- recursive-descent evaluator for the $R[] backreference format ----

class TsrParser:
    def __init__(self, s: str):
        self.s = s
        self.memo = {}
        self.NUM_RE = re.compile(r"-?\d+(\.\d+)?([eE][+-]?\d+)?")
        self.IDENT_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")

    def skip_ws(self, i):
        s = self.s
        while i < len(s) and s[i] in " \t\n\r":
            i += 1
        return i

    def parse_string(self, i):
        s = self.s
        q = s[i]
        j = i + 1
        buf = []
        esc = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "'": "'",
               "`": "`", "\\": "\\", "b": "\b", "f": "\f"}
        while j < len(s):
            if s[j] == "\\":
                nxt = s[j + 1]
                if nxt == "u":
                    buf.append(chr(int(s[j + 2:j + 6], 16)))
                    j += 6
                    continue
                buf.append(esc.get(nxt, nxt))
                j += 2
                continue
            if s[j] == q:
                return "".join(buf), j + 1
            buf.append(s[j])
            j += 1
        raise ValueError("unterminated string")

    def eval_ref(self, i):
        s = self.s
        j = s.index("]", i)
        rid = int(s[i + 3:j])
        k = self.skip_ws(j + 1)
        if k < len(s) and s[k] == "=":
            k = self.skip_ws(k + 1)
            val, end = self.eval_value(k)
            self.memo[rid] = val
            return val, end
        return self.memo.get(rid, {"__unresolved_ref__": rid}), j + 1

    def eval_value(self, i):
        s = self.s
        i = self.skip_ws(i)
        c = s[i]
        if c == "{":
            return self.eval_object(i)
        if c == "[":
            return self.eval_array(i)
        if c in "\"'`":
            return self.parse_string(i)
        if s[i:i + 3] == "$R[":
            return self.eval_ref(i)
        if c == "!":
            return (s[i + 1] == "0"), i + 2
        if s[i:i + 4] == "null":
            return None, i + 4
        if s[i:i + 9] == "undefined":
            return None, i + 9
        if s[i:i + 5] == "void ":
            _, end = self.eval_value(i + 5)
            return None, end
        m = self.NUM_RE.match(s, i)
        if m:
            text = m.group(0)
            num = float(text) if any(ch in text for ch in ".eE") else int(text)
            return num, m.end()
        m = self.IDENT_RE.match(s, i)
        if m:
            return m.group(0), m.end()
        raise ValueError(f"unrecognized value at {i}: ...{s[i:i+60]}...")

    def eval_key(self, i):
        s = self.s
        i = self.skip_ws(i)
        if s[i] in "\"'`":
            return self.parse_string(i)
        m = self.IDENT_RE.match(s, i)
        if m:
            return m.group(0), m.end()
        m = self.NUM_RE.match(s, i)
        if m:
            return m.group(0), m.end()
        raise ValueError(f"unrecognized key at {i}: ...{s[i:i+60]}...")

    def eval_object(self, i):
        s = self.s
        i += 1
        i = self.skip_ws(i)
        d = {}
        if s[i] == "}":
            return d, i + 1
        while True:
            i = self.skip_ws(i)
            key, i = self.eval_key(i)
            i = self.skip_ws(i)
            assert s[i] == ":"
            i += 1
            val, i = self.eval_value(i)
            d[key] = val
            i = self.skip_ws(i)
            if s[i] == ",":
                i += 1
                continue
            if s[i] == "}":
                return d, i + 1
            raise ValueError(f"bad object at {i}: {s[i:i+40]}")

    def eval_array(self, i):
        s = self.s
        i += 1
        i = self.skip_ws(i)
        arr = []
        if s[i] == "]":
            return arr, i + 1
        while True:
            i = self.skip_ws(i)
            val, i = self.eval_value(i)
            arr.append(val)
            i = self.skip_ws(i)
            if s[i] == ",":
                i += 1
                continue
            if s[i] == "]":
                return arr, i + 1
            raise ValueError(f"bad array at {i}: {s[i:i+40]}")


def parse_router_state(script: str):
    p = TsrParser(script)
    idx = script.find("$_TSR.router=")
    if idx == -1:
        raise RuntimeError("$_TSR.router assignment not found")
    start = idx + len("$_TSR.router=")
    start = p.skip_ws(start)
    assert script[start:start + 5] == "($R=>", script[start:start + 20]
    start += 5
    start = p.skip_ws(start)
    root, _ = p.eval_value(start)
    return root


def find_board(root: dict):
    for m in root.get("matches", []):
        l = m.get("l") if isinstance(m, dict) else None
        if isinstance(l, dict) and "predictions" in l:
            return l
    raise RuntimeError("no route match with a 'predictions' loader payload found")


def to_rows(board: dict):
    rows = []
    preds = sorted(board["predictions"], key=lambda p: p.get("pHr", 0), reverse=True)
    for rank, p in enumerate(preds, start=1):
        pitcher = p.get("pitcher") or {}
        park = p.get("park") or {}
        factors = p.get("factors") or {}
        season = p.get("season") or {}
        recent = p.get("recent") or {}
        statcast = p.get("statcast") or {}

        def fv(key):
            f = factors.get(key) or {}
            return f.get("value")

        rows.append({
            "rank": rank,
            "playerId": p.get("playerId"),
            "name": p.get("name"),
            "team": p.get("teamAbbr"),
            "opponent": p.get("opponentAbbr"),
            "isHome": p.get("isHome"),
            "battingOrder": p.get("battingOrder"),
            "position": p.get("position"),
            "bats": p.get("bats"),
            "lineupSource": p.get("lineupSource"),
            "gameStatus": p.get("gameStatus"),
            "pitcher": pitcher.get("name"),
            "pitcherThrows": pitcher.get("throws"),
            "pitcherHr9": pitcher.get("hr9"),
            "pitcherMixLabel": pitcher.get("mixLabel"),
            "pHr": p.get("pHr"),
            "pHrRaw": p.get("pHrRaw"),
            "xHr": p.get("xHr"),
            "expectedPa": p.get("expectedPa"),
            "confidence": p.get("confidence"),
            "confidenceBand": p.get("confidenceBand"),
            "confidenceNotes": "; ".join(p.get("confidenceNotes") or []),
            "reasons": "; ".join(p.get("reasons") or []),
            "factor_batter": fv("batter"),
            "factor_pitcher": fv("pitcher"),
            "factor_park": fv("park"),
            "factor_platoon": fv("platoon"),
            "factor_weather": fv("weather"),
            "factor_form": fv("form"),
            "parkHrFactor": park.get("hrFactor"),
            "parkAirLabel": park.get("airLabel"),
            "season_hr": season.get("hr"),
            "season_pa": season.get("pa"),
            "season_slg": season.get("slg"),
            "recent_hr": recent.get("hr"),
            "recent_pa": recent.get("pa"),
            "recent_games": recent.get("games"),
            "barrelPct": statcast.get("barrel"),
            "evAvg": statcast.get("ev"),
            "hardHitPct": statcast.get("hardHit"),
            "xIso": statcast.get("xIso"),
            "pullPct": statcast.get("pull"),
        })
    return rows


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else _date.today().isoformat()
    html = fetch_html(date_str)
    script = extract_stream_script(html)
    root = parse_router_state(script)
    board = find_board(root)

    out_date = board.get("date", date_str)
    json_path = f"moonshot_{out_date}.json"
    csv_path = f"moonshot_{out_date}.csv"

    with open(json_path, "w") as f:
        json.dump(board, f, indent=2)

    rows = to_rows(board)
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"date: {out_date}  games: {len(board.get('games', []))}  "
          f"predictions: {len(board.get('predictions', []))}")
    print(f"wrote {json_path}")
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()
