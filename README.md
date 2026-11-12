# Multi-LOB Data Pipeline & L2 Support Triage Engine (Airflow • Oracle SQL • Splunk • Grafana)

An enterprise-grade DevOps & L2 Production Support simulation framework built on **RHEL 8**. It models multi-Line of Business (LOB) **Apache Airflow** batch pipelines (`hourly`, `daily`, `weekly`, `monthly`) loading **Oracle SQL** fact/summary tables, emitting **Splunk** key-value (`KV`) log streams and **Grafana/Prometheus** SLA metrics, and running an automated **Python L2 Self-Healing & Triage Watchdog** across isolated `TEST` and `PROD` environments.

---

## Architecture Overview

```text
+-----------------------------------------------------------------------------------+
|                   Multi-LOB Enterprise Data Pipeline Architecture                 |
|                                                                                   |
|  [Apache Airflow Schedules by LOB]                                                |
|   ├── Hourly DAGs    (SLA: 15m) ──> WALLET_TXN          (FACT_HOURLY_TXN)         |
|   ├── Daily DAGs     (SLA: 1h)  ──> MERCHANT_SETTLEMENT (FACT_DAILY_SETTLE)       |
|   ├── Weekly DAGs    (SLA: 2h)  ──> COMPLIANCE_AUDIT    (AGG_WEEKLY_RECON)        |
|   └── Monthly DAGs   (SLA: 4h)  ──> FINANCE_LEDGER      (SUM_MONTHLY_LEDGER)      |
|                                 |                                                 |
|                                 v                                                 |
|  +-----------------------------------------------------------------------------+  |
|  |                 Target Storage Layer: Oracle Database (SQL)                 |  |
|  |  - Staging -> Core Fact/Summary Tables (Validated via SQL Developer)        |  |
|  |  - Row-count integrity, table lock checks (ORA-00054), & data arrival audit |  |
|  +-----------------------------------------------------------------------------+  |
|                                 |                                                 |
|                                 v                                                 |
|  +-----------------------------------------------------------------------------+  |
|  |              Observability & Alerting Layer: Splunk + Grafana               |  |
|  |  1. Splunk KV Logs: Structured log events for SPL triage & alerting         |  |
|  |  2. Grafana Metrics: Prometheus metrics tracking SLA, rows & duration       |  |
|  +-----------------------------------------------------------------------------+  |
|                                 |                                                 |
|                                 v                                                 |
|  +-----------------------------------------------------------------------------+  |
|  |          L2 Automated Triage & Self-Healing Engine (Python on RHEL 8)       |  |
|  |  - Detects FAILED runs, ORA-00054 locks, zero-row loads, & SLA breaches     |  |
|  |  - Auto-clears transient lock failures and retries Airflow tasks            |  |
|  |  - Generates structured RCA JSON reports with SQL Developer triage queries  |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
Repository Structure
Plaintext
cricket-data-pipeline/
├── .github/
│   └── workflows/
│       └── validate-pipelines.yml   # GitHub Actions CI: JSON, syntax & TEST smoke test
├── config/
│   ├── test_lob_schedules.json      # TEST environment SLAs, row thresholds & paths
│   └── prod_lob_schedules.json      # PROD environment strict SLAs & table targets
├── src/
│   ├── airflow_pipeline_sim.py      # Multi-LOB pipeline runner & failure injector
│   └── l2_triage_engine.py          # Automated Splunk/SQL triage & self-healing agent
├── docs/
│   └── L2_TRIAGE_RUNBOOK.md         # Splunk SPL queries, SQL Developer SOP & SLA matrix
├── data/                            # Isolated SQL audit databases (git-ignored)
├── logs/                            # Isolated Splunk logs, Grafana metrics & RCAs (git-ignored)
└── README.md

Quick Start & End-to-End Triage Workflow
1. Execute Healthy Pipelines (TEST & PROD)
Bash
# Validate in TEST environment first
python3 src/airflow_pipeline_sim.py --env test

# Promote execution to PROD environment
python3 src/airflow_pipeline_sim.py --env prod
2. Inject a Real-Time Production Failure (ORA-00054 Table Lock)
Simulate a transient Oracle table lock on the hourly WALLET_TXN pipeline:

Bash
python3 src/airflow_pipeline_sim.py --env test --fail-mode ora-lock --fail-lob WALLET_TXN
Supported --fail-mode scenarios:

ora-lock — Simulates ORA-00054: resource busy and acquire with NOWAIT specified

sla-breach — Simulates pipeline execution exceeding its SLA window

zero-rows — Simulates ORA-01403: no data found in upstream staging feed

3. Run the L2 Automated Triage & Self-Healing Engine
Bash
python3 src/l2_triage_engine.py --env test
The triage engine automatically:

Audits the SQL run history for FAILED or SLA_BREACH states.

Clears transient ORA-00054 lock failures, retries the task, and logs a RECOVERED event to the Splunk log stream.

Generates an incident RCA bundle (logs/test/incidents/INC-TEST-*.json) pre-populated with Splunk SPL queries and Oracle SQL Developer diagnostic queries.

Sample Splunk Log Output (key=value)
Plaintext
timestamp="2026-11-12T09:15:00Z" env="PROD_WALLETPAY" lob="WALLET_TXN" dag_id="dag_wallet_hourly_txn_load" frequency="hourly" target_table="WALLET_CORE.FACT_HOURLY_TXN" rows_loaded=0 duration_sec=27 sla_sec=900 status="FAILED" ora_error="ORA-00054: resource busy and acquire with NOWAIT specified"
timestamp="2026-11-12T09:16:12Z" env="PROD_WALLETPAY" lob="WALLET_TXN" dag_id="dag_wallet_hourly_txn_load" frequency="hourly" target_table="WALLET_CORE.FACT_HOURLY_TXN" rows_loaded=1142 duration_sec=38 status="RECOVERED" action="AUTO_CLEARED_AIRFLOW_TASK_AND_RETRIED_SUCCESS"
Tech Stack
OS & Scripting: Red Hat Enterprise Linux (RHEL 8), Python 3.6+ (standard library only), Bash

Orchestration & Data: Apache Airflow DAG workflows (hourly / daily / weekly / monthly), Oracle SQL / SQL Developer

Observability: Splunk (key=value structured logging & SPL), Grafana / Prometheus metrics

CI/CD & Version Control: Git, GitHub Actions
EOF

* **Verification:** Run `head -n 20 /root/cricket-data-pipeline/README.md` to confirm the file was created cleanly, then stage and push it to GitHub:
  ```bash
  cd /root/cricket-data-pipeline
  git add README.md
  git commit -m "Add architecture README for multi-LOB Airflow, Oracle, Splunk & Grafana triage project"
  git push
