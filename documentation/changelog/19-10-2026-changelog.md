# Changelog: 19-10-2026

- [Monday]-[19/10/2026]-[08:04]-[Dylan Gamero] : Created branch develop and transcribed complete Medallion architecture technical specification v2.0 into documentation/arquitectura-medallion.md with documentation index README.md.
- [Monday]-[19/10/2026]-[08:08]-[Dylan Gamero] : Designed comprehensive User Stories (US-01 to US-15), step-by-step parallel implementation roadmap, synthetic mock data strategy and test protocols in documentation/historias-de-usuario.md.
- [Monday]-[19/10/2026]-[08:24]-[Dylan Gamero] : Removed all temporal day-range references across documentation, structuring roadmap into milestone phases.
- [Monday]-[19/10/2026]-[12:30]-[Héctor] : Implemented US-10 and US-11 on branch feature/airflow-orquestacion: master Airflow DAG dags/dag_medallion_pipeline.py (7 sequential tasks), retries/retry_delay, run_id execution manifests, Gold partition cleanup for idempotent reprocessing, and operational task metrics control table.
- [Monday]-[19/10/2026]-[17:40]-[Héctor] : Extended US-11 observability with per-task SLAs, sla_miss_callback alerting (log/email + SLA_MISS control row) in src/orchestration/sla_monitor.py, exponential retry backoff (max_retry_delay=30m) and python-dotenv in docker-compose; validated E2E on the live Airflow stack (7/7 tasks success) and via unit tests (32 passed).
