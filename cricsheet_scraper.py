#!/usr/bin/env python3
# cricsheet_sample_etl.py
# Downloads a small sample of Cricsheet JSON (4 Tests, 4 ODIs, 4 T20s),
# parses them, and builds a MySQL database.
#
# Usage:
#   python cricsheet_sample_etl.py --out ./data
#
# Requirements: requests, tqdm, mysql-connector-python
#
# Notes:
# - Handles both Cricsheet JSON styles:
#   * v1-style: innings: [{"1st innings": {"team": "...", "deliveries": [{"0.1": {...}}, ...]}}]
#   * v2-style: innings: [{"team": "...", "overs": [{"over": 0, "deliveries": [{...}, ...]}]}]
# - Keeps player/team names as text (no numeric IDs) for simplicity.

import argparse
import io
import os
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from tqdm import tqdm
import mysql.connector
from db_config import config

# -----------------------------
# Config
# -----------------------------
CRICSHEET_BASE = "https://cricsheet.org/downloads"
ZIP_ENDPOINTS = {
    "Test": "tests_json.zip",
    "ODI": "odis_json.zip",
    "T20": "t20s_json.zip",
}
SAMPLE_PER_FORMAT = 4  # 4 * 3 formats = 12 matches

# -----------------------------
# Helpers
# -----------------------------
def split_over_ball(ball_str: str) -> Tuple[Optional[int], Optional[int]]:
    try:
        if "." in ball_str:
            over_s, ball_s = ball_str.split(".")
            return int(over_s), int(ball_s)
        return int(ball_str), 0
    except Exception:
        return None, None

def normalize_date(date_obj) -> Optional[str]:
    if date_obj is None:
        return None
    if isinstance(date_obj, list):
        date_obj = date_obj[0] if date_obj else None
    if isinstance(date_obj, str):
        try:
            dt = datetime.fromisoformat(date_obj)
            return dt.date().isoformat()
        except Exception:
            return date_obj[:10]
    return None

# -----------------------------
# Database Schema
# -----------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS teams (
  team_name VARCHAR(255) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS matches (
  match_id VARCHAR(255) PRIMARY KEY,
  match_type VARCHAR(50),
  match_date DATE,
  venue VARCHAR(255),
  city VARCHAR(255),
  country VARCHAR(100),
  team1 VARCHAR(255),
  team2 VARCHAR(255),
  toss_winner VARCHAR(255),
  toss_decision VARCHAR(50),
  winner VARCHAR(255),
  result VARCHAR(50),
  margin VARCHAR(50),
  FOREIGN KEY(team1) REFERENCES teams(team_name),
  FOREIGN KEY(team2) REFERENCES teams(team_name),
  FOREIGN KEY(toss_winner) REFERENCES teams(team_name),
  FOREIGN KEY(winner) REFERENCES teams(team_name)
);

CREATE TABLE IF NOT EXISTS innings (
  id INT AUTO_INCREMENT PRIMARY KEY,
  match_id VARCHAR(255),
  inning_number INT,
  batting_team VARCHAR(255),
  bowling_team VARCHAR(255),
  runs INT,
  wickets INT,
  overs DECIMAL(5,2),
  FOREIGN KEY(match_id) REFERENCES matches(match_id),
  FOREIGN KEY(batting_team) REFERENCES teams(team_name),
  FOREIGN KEY(bowling_team) REFERENCES teams(team_name)
);

CREATE TABLE IF NOT EXISTS deliveries (
  id INT AUTO_INCREMENT PRIMARY KEY,
  match_id VARCHAR(255),
  inning_number INT,
  overs INT,
  ball INT,
  batting_team VARCHAR(255),
  bowling_team VARCHAR(255),
  batter VARCHAR(255),
  bowler VARCHAR(255),
  non_striker VARCHAR(255),
  runs_batter INT,
  runs_extras INT,
  runs_total INT,
  extras_type VARCHAR(50),
  wicket_kind VARCHAR(50),
  player_out VARCHAR(255),
  FOREIGN KEY(match_id) REFERENCES matches(match_id)
);

CREATE INDEX idx_deliveries_match_inning ON deliveries(match_id, inning_number);
CREATE INDEX idx_deliveries_batter ON deliveries(batter);
CREATE INDEX idx_deliveries_bowler ON deliveries(bowler);
"""

def init_db(config):
    con = mysql.connector.connect(**config)
    cur = con.cursor()
    for stmt in SCHEMA_SQL.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    con.commit()
    return con

def upsert_team(con, team_name: Optional[str]):
    if not team_name:
        return
    cur = con.cursor()
    cur.execute("INSERT IGNORE INTO teams (team_name) VALUES (%s)", (team_name,))
    con.commit()

def insert_match(con, row: dict):
    cols = ",".join(row.keys())
    placeholders = ",".join(["%s"] * len(row))
    cur = con.cursor()
    cur.execute(f"REPLACE INTO matches ({cols}) VALUES ({placeholders})", tuple(row.values()))
    con.commit()

def insert_innings(con, rows: List[dict]):
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ",".join(["%s"] * len(cols))
    cur = con.cursor()
    cur.executemany(
        f"INSERT INTO innings ({','.join(cols)}) VALUES ({placeholders})",
        [tuple(r[c] for c in cols) for r in rows],
    )
    con.commit()

def insert_deliveries(con, rows: List[dict]):
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ",".join(["%s"] * len(cols))
    cur = con.cursor()
    cur.executemany(
        f"INSERT INTO deliveries ({','.join(cols)}) VALUES ({placeholders})",
        [tuple(r[c] for c in cols) for r in rows],
    )
    con.commit()

# -----------------------------
# Parsing JSON
# -----------------------------
def parse_v1_innings(innings_list) -> List[dict]:
    norm_innings = []
    for idx, inn in enumerate(innings_list, start=1):
        if not isinstance(inn, dict) or not inn:
            continue
        name = list(inn.keys())[0]
        data = inn[name]
        team = data.get("team")
        deliveries = []
        for entry in data.get("deliveries", []):
            if not isinstance(entry, dict) or not entry:
                continue
            ball_key = list(entry.keys())[0]
            ev = entry[ball_key]
            over_num, ball_num = split_over_ball(ball_key)
            deliveries.append((over_num, ball_num, ev))
        norm_innings.append({"inning_number": idx, "team": team, "deliveries": deliveries})
    return norm_innings

def parse_v2_innings(innings_list) -> List[dict]:
    norm_innings = []
    for idx, inn in enumerate(innings_list, start=1):
        team = inn.get("team")
        deliveries = []
        for over_blk in inn.get("overs", []):
            over_num = over_blk.get("over")
            for i, ev in enumerate(over_blk.get("deliveries", []), start=1):
                deliveries.append((over_num, i, ev))
        norm_innings.append({"inning_number": idx, "team": team, "deliveries": deliveries})
    return norm_innings

def detect_and_parse_innings(innings_list) -> List[dict]:
    if not innings_list:
        return []
    first = innings_list[0]
    if isinstance(first, dict) and "team" in first and "overs" in first:
        return parse_v2_innings(innings_list)
    return parse_v1_innings(innings_list)

def extract_country_from_venue(info: dict) -> Optional[str]:
    return info.get("country") or None

def parse_match_json(j: dict, match_path: str) -> Tuple[dict, List[dict], List[dict]]:
    info = j.get("info", {})
    match_type = info.get("match_type")
    match_id = info.get("match_id") or os.path.splitext(os.path.basename(match_path))[0]

    teams = info.get("teams", [])
    team1, team2 = (teams + [None, None])[:2]

    match_row = {
        "match_id": match_id,
        "match_type": match_type,
        "match_date": normalize_date(info.get("dates")),
        "venue": info.get("venue"),
        "city": info.get("city"),
        "country": extract_country_from_venue(info),
        "team1": team1,
        "team2": team2,
        "toss_winner": (info.get("toss", {}) or {}).get("winner"),
        "toss_decision": (info.get("toss", {}) or {}).get("decision"),
        "winner": (info.get("outcome", {}) or {}).get("winner"),
        "result": (info.get("outcome", {}) or {}).get("result"),
        "margin": None,
    }

    innings_src = j.get("innings", [])
    norm_innings = detect_and_parse_innings(innings_src)

    innings_rows = []
    deliveries_rows = []

    for inn in norm_innings:
        batting_team = inn.get("team")
        bowling_team = team2 if batting_team == team1 else team1

        total_runs = 0
        total_wkts = 0
        last_over = 0
        last_ball = 0

        for over_num, ball_num, ev in inn["deliveries"]:
            last_over = over_num if over_num is not None else last_over
            last_ball = ball_num if ball_num is not None else last_ball

            runs = (ev.get("runs") or {})
            rb = runs.get("batter", 0)
            rext = runs.get("extras", 0)
            rtot = runs.get("total", rb + rext)

            extras_obj = ev.get("extras") or {}
            extras_type = next(iter(extras_obj.keys()), None) if extras_obj else None

            wickets = ev.get("wickets") or ev.get("wicket")
            wicket_kind = None
            player_out = None
            wicket_flag = 0
            if isinstance(wickets, list) and wickets:
                wicket_flag = 1
                wk = wickets[0]
                wicket_kind = wk.get("kind")
                player_out = wk.get("player_out")

            deliveries_rows.append({
                "match_id": match_id,
                "inning_number": inn["inning_number"],
                "overs": over_num,
                "ball": ball_num,
                "batting_team": batting_team,
                "bowling_team": bowling_team,
                "batter": ev.get("batter"),
                "bowler": ev.get("bowler"),
                "non_striker": ev.get("non_striker"),
                "runs_batter": rb,
                "runs_extras": rext,
                "runs_total": rtot,
                "extras_type": extras_type,
                "wicket_kind": wicket_kind,
                "player_out": player_out,
            })

            total_runs += rtot
            total_wkts += wicket_flag

        overs_float = None
        if last_over is not None and last_ball is not None:
            overs_float = (last_over or 0) + (last_ball or 0) / 6.0

        innings_rows.append({
            "match_id": match_id,
            "inning_number": inn["inning_number"],
            "batting_team": batting_team,
            "bowling_team": bowling_team,
            "runs": total_runs,
            "wickets": total_wkts,
            "overs": overs_float,
        })

    return match_row, innings_rows, deliveries_rows

# -----------------------------
# Download & Extract
# -----------------------------
def download_zip(url: str) -> bytes:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                buf.write(chunk)
        return buf.getvalue()

def collect_sample_files(zip_bytes: bytes, sample_n: int) -> List[Tuple[str, bytes]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = sorted([n for n in zf.namelist() if n.lower().endswith(".json")])
        for name in names[:sample_n]:
            out.append((name, zf.read(name)))
    return out

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Download and parse a dozen Cricsheet matches into MySQL.")
    ap.add_argument("--out", default="./cricsheet_sample", help="Output folder for JSONs")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    con = init_db(config)

    total_matches = 0

    for fmt, zip_name in ZIP_ENDPOINTS.items():
        url = f"{CRICSHEET_BASE}/{zip_name}"
        print(f"Downloading {fmt} zip from: {url}")
        zbytes = download_zip(url)
        print(f"Downloaded {len(zbytes):,} bytes. Extracting sample files...")
        samples = collect_sample_files(zbytes, SAMPLE_PER_FORMAT)
        for name, fbytes in samples:
            total_matches += 1
            out_json_path = os.path.join(args.out, os.path.basename(name))
            with open(out_json_path, "wb") as f:
                f.write(fbytes)
            try:
                import json
                j = json.loads(fbytes.decode("utf-8"))
                match_row, innings_rows, deliveries_rows = parse_match_json(j, out_json_path)

                for t in [match_row.get("team1"), match_row.get("team2"),
                          match_row.get("toss_winner"), match_row.get("winner")]:
                    upsert_team(con, t)

                insert_match(con, match_row)
                insert_innings(con, innings_rows)
                insert_deliveries(con, deliveries_rows)
                print(f"Loaded: {name} -> match_id={match_row['match_id']} ({fmt}) | innings={len(innings_rows)} | deliveries={len(deliveries_rows)}")
            except Exception as e:
                print(f"Failed to parse {name}: {e}")

    print(f"\n✅ Done. Matches processed: {total_matches} into MySQL database: {config['database']}")
    print("Sample queries you can try:")
    print("  SELECT match_type, COUNT(*) FROM matches GROUP BY match_type;")
    print("  SELECT batter, SUM(runs_batter) AS runs FROM deliveries GROUP BY batter ORDER BY runs DESC LIMIT 10;")
    print("  SELECT bowler, SUM(CASE WHEN wicket_kind IS NOT NULL THEN 1 ELSE 0 END) AS wickets FROM deliveries GROUP BY bowler ORDER BY wickets DESC LIMIT 10;")

if __name__ == "__main__":
    main()