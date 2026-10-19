"""Compuerta de calidad numérica sobre las métricas Gold (US-10, tarea quality_gate).

Valida que los KPIs agregados no presenten anomalías numéricas evidentes antes
de publicar el resultado final de la ejecución.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class QualityGateResult:
    """Resultado de la evaluación de la compuerta de calidad Gold."""

    passed: bool
    checked_metrics: int
    anomalies: List[str] = field(default_factory=list)

    def summary(self) -> str:
        if self.passed:
            return f"Quality gate superado ({self.checked_metrics} métricas validadas)."
        return "Quality gate fallido: " + "; ".join(self.anomalies)


class GoldQualityGate:
    """Reglas mínimas de sanidad sobre los KPIs de la capa Gold."""

    def __init__(self, max_revenue_per_day: float = 1_000_000.00) -> None:
        self.max_revenue_per_day = max_revenue_per_day

    def evaluate(self, kpi_rows: Optional[List[Dict[str, Any]]]) -> QualityGateResult:
        """Evalúa una lista de filas KPI (daily_sales) y reporta anomalías numéricas."""
        rows = kpi_rows or []
        anomalies: List[str] = []

        if not rows:
            return QualityGateResult(passed=False, checked_metrics=0, anomalies=["Sin KPIs Gold generados."])

        for row in rows:
            kpi_date = str(row.get("kpi_date", "desconocida"))
            revenue = float(row.get("total_revenue", 0.0) or 0.0)
            total_orders = int(row.get("total_orders", 0) or 0)
            completed = int(row.get("completed_orders", 0) or 0)
            cancelled = int(row.get("cancelled_orders", 0) or 0)

            if revenue < 0:
                anomalies.append(f"{kpi_date}: total_revenue negativo ({revenue}).")
            if revenue > self.max_revenue_per_day:
                anomalies.append(f"{kpi_date}: total_revenue fuera de rango ({revenue}).")
            if total_orders < 0:
                anomalies.append(f"{kpi_date}: total_orders negativo ({total_orders}).")
            if completed + cancelled > total_orders:
                anomalies.append(
                    f"{kpi_date}: órdenes completadas+canceladas ({completed + cancelled}) "
                    f"superan el total ({total_orders})."
                )

        return QualityGateResult(passed=not anomalies, checked_metrics=len(rows), anomalies=anomalies)
