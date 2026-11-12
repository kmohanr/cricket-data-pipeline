#!/usr/bin/env python3
"""
Multi-LOB Apache Airflow Pipeline Simulator (TEST & PROD)
Loads Oracle-style staging/fact tables, writes Splunk KV logs, and exports Grafana metrics.
"""

import argparse
import datetime
import json
import os
import random
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env_config(env_name):
    cfg_path = os.path.join(BASE_DIR, "config", f"{env_name}_lob_schedules.json")
    if not os.path.exists(cfg_path):
        print(f"ERROR: Config not found for environment '{env_name}': {cfg_path}")
        sys.exit(1)
    with open(cfg_path, "r") as f:
        return json.load(f)


def init_oracle_db(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS PIPELINE_RUN_AUDIT (
            run_id TEXT PRIMARY KEY,
            env TEXT,
            lob TEXT,
            dag_id TEXT,
            frequency TEXT,
            target_table TEXT,
            rows_loaded INTEGER,
            duration_sec INTEGER,
            status TEXT,
            ora_error TEXT,
            run_timestamp_utc TEXT
        )
    """)
    conn.commit()
    return conn


def run_pipelines(env_name, target_freq="all", fail_mode="none", fail_lob=None):
    cfg = load_env_config(env_name)
    env_label = cfg["environment"]
    db_path = os.path.join(BASE_DIR, cfg["oracle_sim_db"])
    splunk_log = os.path.join(BASE_DIR, cfg["splunk_log_file"])
    grafana_prom = os.path.join(BASE_DIR, cfg["grafana_metrics_file"])

    os.makedirs(os.path.dirname(splunk_log), exist_ok=True)
    os.makedirs(os.path.dirname(grafana_prom), exist_ok=True)

    conn = init_oracle_db(db_path)
    cur = conn.cursor()

    now = datetime.datetime.utcnow()
    ts_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    prom_lines = [
        "# HELP airflow_lob_pipeline_status 1=SUCCESS, 0=FAILED",
        "# TYPE airflow_lob_pipeline_status gauge",
        "# HELP airflow_lob_rows_loaded Rows inserted into Oracle target table",
        "# TYPE airflow_lob_rows_loaded gauge",
        "# HELP airflow_lob_duration_seconds Pipeline execution duration in seconds",
        "# TYPE airflow_lob_duration_seconds gauge",
    ]

    for p in cfg["pipelines"]:
        if target_freq != "all" and p["frequency"] != target_freq:
            continue

        lob = p["lob"]
        dag_id = p["dag_id"]
        freq = p["frequency"]
        table = p["target_table"]
        sla = p["sla_seconds"]
        min_rows = p["min_expected_rows"]

        inject_here = fail_mode != "none" and (fail_lob is None or fail_lob == lob)

        if inject_here and fail_mode == "ora-lock":
            status = "FAILED"
            ora_err = "ORA-00054: resource busy and acquire with NOWAIT specified"
            rows_loaded = 0
            duration = random.randint(15, 45)
        elif inject_here and fail_mode == "sla-breach":
            status = "SLA_BREACH"
            ora_err = "NONE"
            rows_loaded = min_rows + random.randint(50, 300)
            duration = sla + 420
        elif inject_here and fail_mode == "zero-rows":
            status = "FAILED"
            ora_err = "ORA-01403: no data found in upstream staging feed"
            rows_loaded = 0
            duration = random.randint(10, 30)
        else:
            status = "SUCCESS"
            ora_err = "NONE"
            rows_loaded = min_rows + random.randint(100, 1500)
            duration = random.randint(max(5, int(sla * 0.1)), max(15, int(sla * 0.4)))

        run_id = f"{env_name.upper()}_{lob}_{now.strftime('%Y%m%d%H%M%S')}"
        cur.execute(
            """
            INSERT OR REPLACE INTO PIPELINE_RUN_AUDIT
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, env_label, lob, dag_id, freq, table, rows_loaded, duration, status, ora_err, ts_str),
        )

        # Write Splunk Key-Value Log Line
        splunk_event = (
            f'timestamp="{ts_str}" env="{env_label}" lob="{lob}" dag_id="{dag_id}" '
            f'frequency="{freq}" target_table="{table}" rows_loaded={rows_loaded} '
            f'duration_sec={duration} sla_sec={sla} status="{status}" ora_error="{ora_err}"\n'
        )
        with open(splunk_log, "a") as sf:
            sf.write(splunk_event)

        # Append Grafana / Prometheus Metrics
        status_val = 1 if status == "SUCCESS" else 0
        labels = f'env="{env_label}",lob="{lob}",dag_id="{dag_id}",frequency="{freq}",table="{table}"'
        prom_lines.append(f"airflow_lob_pipeline_status{{{labels}}} {status_val}")
        prom_lines.append(f"airflow_lob_rows_loaded{{{labels}}} {rows_loaded}")
        prom_lines.append(f"airflow_lob_duration_seconds{{{labels}}} {duration}")

        print(f"[{env_label}] {freq.upper():7s} | {lob:20s} | {status:10s} | rows={rows_loaded} | dur={duration}s")

    conn.commit()
    conn.close()

    with open(grafana_prom, "w") as gf:
        gf.write("\n".join(prom_lines) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Multi-LOB Airflow Pipeline Simulator")
    parser.add_argument("--env", choices=["test", "prod"], required=True, help="Target environment (test or prod)")
    parser.add_argument("--freq", choices=["all", "hourly", "daily", "weekly", "monthly"], default="all")
    parser.add_argument("--fail-mode", choices=["none", "ora-lock", "sla-breach", "zero-rows"], default="none")
    parser.add_argument("--fail-lob", default=None, help="Specific LOB to inject failure into")
    args = parser.parse_args()

    run_pipelines(args.env, args.freq, args.fail_mode, args.fail_lob)
