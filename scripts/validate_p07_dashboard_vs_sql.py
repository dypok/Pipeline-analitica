"""Validación cruzada P06/P07: KPIs del dashboard vs SQL directo con DuckDB.

Compara `dashboards.kpi_calculator.compute_global_kpis` (lo que muestra la UI)
contra un ``SELECT SUM(...)`` real sobre ``data/mock/gold/daily_sales.parquet``.
Cualquier diferencia != 0 invalida los criterios de aceptación P06/P07.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

# Raíz del proyecto en sys.path (ejecución directa sin instalar el paquete).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dashboards.kpi_calculator import compute_global_kpis

GOLD = "data/mock/gold/daily_sales.parquet"

# 1) KPIs calculados por la capa del dashboard (Pandas)
df = pd.read_parquet(GOLD)
ui = compute_global_kpis(df)

# 2) Mismas cifras vía SQL directo sobre el Parquet (fuente "oficial", P07)
con = duckdb.connect()
row = con.execute(f"""
    SELECT
        SUM(total_revenue)   AS total_revenue,
        SUM(total_orders)    AS total_orders,
        SUM(completed_orders) AS completed_orders,
        ROUND(SUM(total_revenue) / NULLIF(SUM(completed_orders), 0), 2) AS avg_ticket
    FROM read_parquet('{GOLD}')
""").fetchone()
sql = {
    "total_revenue": round(float(row[0]), 2),
    "total_orders": int(row[1]),
    "completed_orders": int(row[2]),
    "average_ticket": round(float(row[3]), 2),
}

print(f"Filas daily_sales: {len(df)}")
print(f"{'METRICA':<20}{'DASHBOARD':>18}{'SQL (DuckDB)':>18}{'DELTA':>10}")
print("-" * 66)

ok = True
for key in ("total_revenue", "total_orders", "average_ticket"):
    delta = abs(ui[key] - sql[key])
    status = "OK" if delta < 0.01 else "MISMATCH"
    if delta >= 0.01:
        ok = False
    print(f"{key:<20}{ui[key]:>18,}{sql[key]:>18,}{delta:>10.2f}  {status}")

print("-" * 66)
print("P07 PASA [OK]" if ok else "P07 FALLA [MISMATCH] (KPIs del dashboard no cuadran con SQL)")
