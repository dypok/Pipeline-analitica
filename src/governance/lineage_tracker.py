"""Rastreador de linaje de datos de extremo a extremo (US-09).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Permite modelar, consultar y exportar el grafo de dependencias de datos:
CSV / API -> Landing -> Bronze -> Silver -> Gold (daily_sales, customer_360)
así como la segregación hacia Quarantine. Compatible con OpenMetadata y Apache Atlas.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class LineageNode:
    """Nodo en el grafo de linaje de datos."""

    id: str
    name: str
    layer: str  # SOURCE, LANDING, BRONZE, SILVER, GOLD, QUARANTINE, CONSUMPTION
    description: str


@dataclass
class LineageEdge:
    """Arista dirigida que representa transformación o flujo de datos."""

    from_node: str
    to_node: str
    transformation_type: str  # INGESTION, ENRICHMENT, QUALITY_GATE, AGGREGATION, CONSUMPTION
    description: str


class LineageTracker:
    """Motor de gestión y trazabilidad del linaje de datos del Lakehouse."""

    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self._build_default_lineage()

    def add_node(self, node_id: str, name: str, layer: str, description: str):
        self.nodes[node_id] = LineageNode(node_id, name, layer, description)

    def add_edge(self, from_id: str, to_id: str, transformation_type: str, description: str):
        self.edges.append(LineageEdge(from_id, to_id, transformation_type, description))

    def _build_default_lineage(self):
        """Construye el grafo maestro de linaje de la plataforma Medallion."""
        # 1. Fuentes externas
        self.add_node("src.csv_orders", "CSV Orders Feed", "SOURCE", "Archivos CSV crudos de órdenes")
        self.add_node("src.api_sales", "REST API Sales Stream", "SOURCE", "Endpoint transaccional de ventas")
        self.add_node("src.csv_customers", "CSV Customers Master", "SOURCE", "Catálogo crudo de clientes")
        self.add_node("src.csv_products", "CSV Products Catalog", "SOURCE", "Maestro de productos y precios")
        self.add_node("src.csv_returns", "CSV Returns Log", "SOURCE", "Registro de devoluciones de retail")

        # 2. Capa Landing (MinIO s3://landing)
        self.add_node("landing.orders", "Landing Orders", "LANDING", "Depósito inmutable de órdenes crudas")
        self.add_node("landing.customers", "Landing Customers", "LANDING", "Depósito inmutable de clientes")
        self.add_node("landing.products", "Landing Products", "LANDING", "Depósito inmutable de productos")
        self.add_node("landing.returns", "Landing Returns", "LANDING", "Depósito inmutable de devoluciones")

        # 3. Capa Bronze (MinIO s3://bronze)
        self.add_node("bronze.orders", "Bronze Orders", "BRONZE", "Crudo + metadatos técnicos SHA256 y UUID")
        self.add_node("bronze.customers", "Bronze Customers", "BRONZE", "Clientes + envelope de auditoría")
        self.add_node("bronze.products", "Bronze Products", "BRONZE", "Productos + metadatos de linaje")
        self.add_node("bronze.returns", "Bronze Returns", "BRONZE", "Devoluciones + timestamp de ingesta")

        # 4. Zona Quarantine (s3://quarantine)
        self.add_node("quarantine.anomalies", "Quarantine Anomalies", "QUARANTINE", "Registros anómalos aislados por reglas de negocio")

        # 5. Capa Silver (MinIO s3://silver - Parquet)
        self.add_node("silver.orders", "Silver Orders (Parquet)", "SILVER", "Órdenes tipadas, limpias y deduplicadas")
        self.add_node("silver.customers", "Silver Customers (Parquet)", "SILVER", "Clientes normalizados con PII gobernado")
        self.add_node("silver.products", "Silver Products (Parquet)", "SILVER", "Catálogo limpio con precios validados")
        self.add_node("silver.returns", "Silver Returns (Parquet)", "SILVER", "Devoluciones tipadas y normalizadas")

        # 6. Capa Gold (MinIO s3://gold y PostgreSQL analytics_db)
        self.add_node("gold.daily_sales", "Gold Daily Sales KPIs", "GOLD", "KPIs agregados diarios por país y categoría")
        self.add_node("gold.customer_360", "Gold Customer 360", "GOLD", "Modelo analítico RFM y LTV por cliente")
        self.add_node("gold.product_performance", "Gold Product Performance", "GOLD", "Márgenes y tasas de devolución por producto")

        # 7. Capa de Consumo Final
        self.add_node("consumption.dashboard_sales", "Streamlit Sales Dashboard", "CONSUMPTION", "Vistas interactivas de ventas y KPIs")
        self.add_node("consumption.chatbot_ai", "FastAPI / LLM Analytics Chatbot", "CONSUMPTION", "Consultas conversacionales con DuckDB")

        # Aristas: Ingesta (Fuentes -> Landing)
        self.add_edge("src.csv_orders", "landing.orders", "INGESTION", "file_ingestor.py carga directa")
        self.add_edge("src.api_sales", "landing.orders", "INGESTION", "REST connector de ventas")
        self.add_edge("src.csv_customers", "landing.customers", "INGESTION", "Ingesta de clientes")
        self.add_edge("src.csv_products", "landing.products", "INGESTION", "Ingesta de catálogo")
        self.add_edge("src.csv_returns", "landing.returns", "INGESTION", "Ingesta de devoluciones")

        # Aristas: Landing -> Bronze
        self.add_edge("landing.orders", "bronze.orders", "ENRICHMENT", "BronzeLoader añade hash SHA256")
        self.add_edge("landing.customers", "bronze.customers", "ENRICHMENT", "BronzeLoader añade UUID de ingesta")
        self.add_edge("landing.products", "bronze.products", "ENRICHMENT", "BronzeLoader añade metadatos")
        self.add_edge("landing.returns", "bronze.returns", "ENRICHMENT", "BronzeLoader añade metadatos")

        # Aristas: Bronze -> Silver y Quarantine (Quality Gate)
        self.add_edge("bronze.orders", "silver.orders", "TRANSFORMATION", "SilverTransformer tipado y deduplicación")
        self.add_edge("bronze.orders", "quarantine.anomalies", "QUALITY_GATE", "Filas que fallan validación van a Quarantine")
        self.add_edge("bronze.customers", "silver.customers", "TRANSFORMATION", "Normalización de emails y teléfonos")
        self.add_edge("bronze.customers", "quarantine.anomalies", "QUALITY_GATE", "Emails malformados segregados")
        self.add_edge("bronze.products", "silver.products", "TRANSFORMATION", "Tipado de unit_price y cost_price")
        self.add_edge("bronze.products", "quarantine.anomalies", "QUALITY_GATE", "Precios negativos aislados")
        self.add_edge("bronze.returns", "silver.returns", "TRANSFORMATION", "Normalización de reembolsos")

        # Aristas: Silver -> Gold
        self.add_edge("silver.orders", "gold.daily_sales", "AGGREGATION", "GoldAggregator agrupa por fecha, país, categoría")
        self.add_edge("silver.returns", "gold.daily_sales", "AGGREGATION", "Cruza devoluciones para calcular reembolsos")

        self.add_edge("silver.orders", "gold.customer_360", "AGGREGATION", "Cálculo de LTV, ticket promedio y frecuencia")
        self.add_edge("silver.customers", "gold.customer_360", "AGGREGATION", "Dimensión de cliente y fecha de registro")

        self.add_edge("silver.orders", "gold.product_performance", "AGGREGATION", "Conteo de unidades vendidas e ingresos")
        self.add_edge("silver.products", "gold.product_performance", "AGGREGATION", "Cálculo de margen comercial")
        self.add_edge("silver.returns", "gold.product_performance", "AGGREGATION", "Cálculo de return_rate por producto")

        # Aristas: Gold -> Consumo
        self.add_edge("gold.daily_sales", "consumption.dashboard_sales", "CONSUMPTION", "Streamlit lee tabla daily_sales_kpis")
        self.add_edge("gold.daily_sales", "consumption.chatbot_ai", "CONSUMPTION", "DuckDB vista v_daily_sales")
        self.add_edge("gold.customer_360", "consumption.chatbot_ai", "CONSUMPTION", "DuckDB vista v_customer_360")
        self.add_edge("gold.product_performance", "consumption.chatbot_ai", "CONSUMPTION", "DuckDB vista v_product_performance")

    def get_upstream_lineage(self, target_node_id: str) -> List[str]:
        """Obtiene la lista de todos los nodos ancestros (upstream) de un nodo destino.

        Prueba T09.1 / P10: daily_sales debe rastrearse hasta las fuentes originales CSV/API.
        """
        visited: Set[str] = set()
        queue = [target_node_id]

        while queue:
            curr = queue.pop(0)
            for edge in self.edges:
                if edge.to_node == curr and edge.from_node not in visited:
                    visited.add(edge.from_node)
                    queue.append(edge.from_node)

        return list(visited)

    def get_downstream_lineage(self, source_node_id: str) -> List[str]:
        """Obtiene todos los nodos descendientes (downstream) impactados por una fuente."""
        visited: Set[str] = set()
        queue = [source_node_id]

        while queue:
            curr = queue.pop(0)
            for edge in self.edges:
                if edge.from_node == curr and edge.to_node not in visited:
                    visited.add(edge.to_node)
                    queue.append(edge.to_node)

        return list(visited)

    def generate_mermaid_diagram(self) -> str:
        """Genera la representación gráfica del linaje en sintaxis Mermaid."""
        lines = ["flowchart LR"]
        for edge in self.edges:
            f_clean = edge.from_node.replace(".", "_")
            t_clean = edge.to_node.replace(".", "_")
            f_label = self.nodes.get(edge.from_node, LineageNode(edge.from_node, edge.from_node, "", "")).name
            t_label = self.nodes.get(edge.to_node, LineageNode(edge.to_node, edge.to_node, "", "")).name
            lines.append(f'    {f_clean}["{f_label}"] -->|"{edge.transformation_type}"| {t_clean}["{t_label}"]')
        return "\n".join(lines)

    def export_openmetadata_lineage(self) -> Dict[str, Any]:
        """Genera el manifiesto de linaje en formato compatible con OpenMetadata."""
        edges_payload = []
        for edge in self.edges:
            edges_payload.append({
                "fromEntity": {
                    "id": edge.from_node,
                    "type": "table",
                    "fullyQualifiedName": f"medallion.{edge.from_node}",
                },
                "toEntity": {
                    "id": edge.to_node,
                    "type": "table",
                    "fullyQualifiedName": f"medallion.{edge.to_node}",
                },
                "description": edge.description,
            })
        return {
            "entity": "medallion_lakehouse",
            "nodesCount": len(self.nodes),
            "edgesCount": len(self.edges),
            "lineageEdges": edges_payload,
        }
