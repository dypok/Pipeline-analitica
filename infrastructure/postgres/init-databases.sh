#!/bin/bash
set -e

echo "=== [PostgreSQL Init] Inicializando bases de datos múltiples ==="

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow_db'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

    SELECT 'CREATE DATABASE analytics_db'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'analytics_db')\gexec

    GRANT ALL PRIVILEGES ON DATABASE airflow_db TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE analytics_db TO $POSTGRES_USER;
EOSQL

echo "=== [PostgreSQL Init] Configurando schema y tablas en analytics_db ==="
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "analytics_db" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS analytics;

    -- Tabla analítica Gold: Indicadores diarios de ventas (consumida por Dashboards y Chatbot)
    CREATE TABLE IF NOT EXISTS analytics.daily_sales_kpis (
        kpi_date DATE PRIMARY KEY,
        total_orders INT NOT NULL,
        completed_orders INT NOT NULL,
        cancelled_orders INT NOT NULL,
        total_revenue NUMERIC(12, 2) NOT NULL,
        average_ticket NUMERIC(10, 2) NOT NULL,
        distinct_customers INT NOT NULL,
        top_category VARCHAR(100),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    -- Tabla analítica Gold: Vista 360 del Cliente
    CREATE TABLE IF NOT EXISTS analytics.customer_360_kpis (
        customer_id VARCHAR(50) PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(150),
        country VARCHAR(50),
        total_lifetime_orders INT DEFAULT 0,
        total_lifetime_spend NUMERIC(12, 2) DEFAULT 0.00,
        average_order_value NUMERIC(10, 2) DEFAULT 0.00,
        first_order_date DATE,
        last_order_date DATE,
        customer_tier VARCHAR(20) DEFAULT 'STANDARD',
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    -- Tabla de auditoría: registro de ejecuciones de pipelines Medallion
    CREATE TABLE IF NOT EXISTS analytics.pipeline_execution_logs (
        execution_id VARCHAR(100) PRIMARY KEY,
        pipeline_name VARCHAR(100) NOT NULL,
        layer VARCHAR(20) NOT NULL,
        status VARCHAR(20) NOT NULL,
        records_processed INT DEFAULT 0,
        records_quarantined INT DEFAULT 0,
        started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP WITH TIME ZONE,
        error_message TEXT
    );

    -- Tabla de control operativa: métricas por tarea y corrida de Airflow (US-11)
    CREATE TABLE IF NOT EXISTS analytics.pipeline_task_metrics (
        run_id VARCHAR(200) NOT NULL,
        task_id VARCHAR(100) NOT NULL,
        pipeline_name VARCHAR(100) NOT NULL,
        layer VARCHAR(20),
        status VARCHAR(20) NOT NULL,
        duration_seconds NUMERIC(12, 3) DEFAULT 0,
        records_processed INT DEFAULT 0,
        records_quarantined INT DEFAULT 0,
        recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (run_id, task_id)
    );

    CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON analytics.daily_sales_kpis(kpi_date);
    CREATE INDEX IF NOT EXISTS idx_customer_tier ON analytics.customer_360_kpis(customer_tier);
    CREATE INDEX IF NOT EXISTS idx_task_metrics_run ON analytics.pipeline_task_metrics(run_id);
EOSQL

echo "=== [PostgreSQL Init] Inicialización completada con éxito ==="
