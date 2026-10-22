"""Agregador de la capa Gold: Cálculo de modelos analíticos multidimensionales,
Parquet optimizado en MinIO y sincronización con PostgreSQL."""

from datetime import datetime, date, timezone
import hashlib
import io
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import get_settings
from src.common.db_client import DatabaseClient
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client

logger = get_logger("GoldAggregator")


class GoldAggregator:
    """Calcula indicadores y modelos analíticos de la capa Gold:
    - daily_sales (Grano: order_date, shipping_country, category)
    - customer_360 (Grano: customer_id con RFM y PII protegido)
    - product_performance (Grano: product_id con márgenes y tasas de retorno)
    """

    def __init__(self, s3_client: Optional[S3Client] = None, db_client: Optional[DatabaseClient] = None):
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.db_client = db_client or DatabaseClient()

    def build_daily_sales(
        self,
        df_orders: pd.DataFrame,
        df_returns: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Construye el modelo analítico daily_sales a grano (order_date, shipping_country, category)."""
        if df_orders.empty:
            return pd.DataFrame(columns=[
                "order_date", "shipping_country", "category",
                "total_revenue", "orders_count", "units_sold",
                "avg_ticket", "returns_count", "refunded_amount",
            ])

        df = df_orders.copy()
        if "status" not in df.columns:
            if "order_status" in df.columns:
                df["status"] = df["order_status"].astype(str).str.upper()
            else:
                df["status"] = "COMPLETED"
        else:
            df["status"] = df["status"].astype(str).str.upper()

        if "total_amount" not in df.columns:
            if "unit_price" in df.columns and "quantity" in df.columns:
                df["total_amount"] = (df["unit_price"] * df["quantity"]).round(2)
            else:
                df["total_amount"] = 0.0

        if "order_date" in df.columns:
            df["order_date"] = df["order_date"].astype(str).str.slice(0, 10)

        if "shipping_country" not in df.columns:
            df["shipping_country"] = "GLOBAL"
        if "category" not in df.columns:
            df["category"] = "General"
        if "quantity" not in df.columns:
            df["quantity"] = 1

        # Filtrar órdenes completadas para métricas de ingresos oficiales
        completed_df = df[df["status"] == "COMPLETED"]

        if completed_df.empty:
            # Fallback a todas si no hay completadas
            completed_df = df

        grp = completed_df.groupby(["order_date", "shipping_country", "category"]).agg(
            total_revenue=("total_amount", "sum"),
            orders_count=("order_id", "nunique"),
            units_sold=("quantity", "sum"),
        ).reset_index()

        grp["total_revenue"] = grp["total_revenue"].round(2)
        grp["avg_ticket"] = (grp["total_revenue"] / grp["orders_count"]).round(2)

        # Integración con devoluciones si están disponibles
        if df_returns is not None and not df_returns.empty:
            ret = df_returns.copy()
            # Si retorna order_id, cruzamos para saber shipping_country y category
            if "shipping_country" not in ret.columns or "category" not in ret.columns:
                ret = ret.merge(
                    df[["order_id", "shipping_country", "category"]].drop_duplicates(),
                    on="order_id",
                    how="left",
                )
            ret["shipping_country"] = ret["shipping_country"].fillna("GLOBAL")
            ret["category"] = ret["category"].fillna("General")
            if "return_date" in ret.columns:
                ret["order_date"] = ret["return_date"]

            ret_agg = ret.groupby(["order_date", "shipping_country", "category"]).agg(
                returns_count=("return_id", "count"),
                refunded_amount=("refund_amount", "sum"),
            ).reset_index()

            res = grp.merge(ret_agg, on=["order_date", "shipping_country", "category"], how="left")
            res["returns_count"] = res["returns_count"].fillna(0).astype("int32")
            res["refunded_amount"] = res["refunded_amount"].fillna(0.0).round(2)
        else:
            res = grp
            res["returns_count"] = 0
            res["refunded_amount"] = 0.0

        res["_gold_calculated_at"] = datetime.now(timezone.utc).isoformat()
        return res

    def build_customer_360(
        self,
        df_orders: pd.DataFrame,
        df_customers: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Construye el modelo customer_360 con métricas LTV, segmentación RFM y minimización PII."""
        if df_orders.empty:
            return pd.DataFrame()

        df = df_orders.copy()
        if "status" not in df.columns:
            if "order_status" in df.columns:
                df["status"] = df["order_status"].astype(str).str.upper()
            else:
                df["status"] = "COMPLETED"
        else:
            df["status"] = df["status"].astype(str).str.upper()

        if "total_amount" not in df.columns:
            if "unit_price" in df.columns and "quantity" in df.columns:
                df["total_amount"] = (df["unit_price"] * df["quantity"]).round(2)
            else:
                df["total_amount"] = 0.0

        if "order_date" in df.columns:
            df["order_date"] = df["order_date"].astype(str).str.slice(0, 10)

        completed = df[df["status"] == "COMPLETED"]
        if completed.empty:
            completed = df

        if "quantity" not in completed.columns:
            completed["quantity"] = 1

        # Métricas de compras por cliente
        cust_agg = completed.groupby("customer_id").agg(
            first_purchase_date=("order_date", "min"),
            last_purchase_date=("order_date", "max"),
            total_orders=("order_id", "nunique"),
            lifetime_value=("total_amount", "sum"),
            units_bought=("quantity", "sum"),
        ).reset_index()

        cust_agg["lifetime_value"] = cust_agg["lifetime_value"].round(2)
        cust_agg["avg_order_value"] = (cust_agg["lifetime_value"] / cust_agg["total_orders"]).round(2)

        # Categoría favorita
        if "category" in completed.columns:
            fav_cat = completed.groupby(["customer_id", "category"])["quantity"].sum().reset_index()
            fav_cat = fav_cat.sort_values(["customer_id", "quantity"], ascending=[True, False]).drop_duplicates(
                subset=["customer_id"]
            )
            fav_cat = fav_cat.rename(columns={"category": "favorite_category"})[["customer_id", "favorite_category"]]
            cust_agg = cust_agg.merge(fav_cat, on="customer_id", how="left")
        else:
            cust_agg["favorite_category"] = "General"

        # Segmentación RFM (Recency, Frequency, Monetary)
        max_date = pd.to_datetime(cust_agg["last_purchase_date"]).max()
        if pd.isna(max_date):
            max_date = pd.to_datetime(date.today())

        cust_agg["recency_days"] = (max_date - pd.to_datetime(cust_agg["last_purchase_date"])).dt.days

        def assign_rfm_segment(row):
            rec = row["recency_days"]
            freq = row["total_orders"]
            mon = row["lifetime_value"]
            if rec <= 30 and freq >= 5:
                return "Champions"
            elif rec <= 60 and freq >= 3:
                return "Loyal Customers"
            elif rec <= 90 and mon >= 500:
                return "Potential Loyalist"
            elif rec > 120:
                return "At Risk"
            else:
                return "Occasional / New"

        cust_agg["rfm_segment"] = cust_agg.apply(assign_rfm_segment, axis=1)

        # Enriquecimiento y Protección de Datos Personales (PII Minimization)
        if df_customers is not None and not df_customers.empty:
            cust_meta = df_customers.copy()
            keep_cols = ["customer_id"]
            for col_name in ["country", "city", "registration_date"]:
                if col_name in cust_meta.columns:
                    keep_cols.append(col_name)

            # Enmascaramiento PII con Hashing SHA-256
            if "email" in cust_meta.columns:
                cust_meta["email_hash"] = cust_meta["email"].astype(str).apply(
                    lambda e: hashlib.sha256(e.strip().lower().encode("utf-8")).hexdigest()
                )
                keep_cols.append("email_hash")
            if "phone" in cust_meta.columns:
                cust_meta["phone_masked"] = cust_meta["phone"].astype(str).apply(
                    lambda p: f"+***-***-{p.strip()[-4:]}" if len(p.strip()) >= 4 else "masked"
                )
                keep_cols.append("phone_masked")

            cust_agg = cust_agg.merge(cust_meta[keep_cols].drop_duplicates("customer_id"), on="customer_id", how="left")

        cust_agg["_gold_calculated_at"] = datetime.now(timezone.utc).isoformat()
        return cust_agg

    def build_product_performance(
        self,
        df_orders: pd.DataFrame,
        df_products: Optional[pd.DataFrame] = None,
        df_returns: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Construye el modelo product_performance con márgenes, retornos y clasificación."""
        if df_orders.empty:
            return pd.DataFrame()

        df = df_orders.copy()
        if "status" not in df.columns:
            if "order_status" in df.columns:
                df["status"] = df["order_status"].astype(str).str.upper()
            else:
                df["status"] = "COMPLETED"
        else:
            df["status"] = df["status"].astype(str).str.upper()

        if "total_amount" not in df.columns:
            if "unit_price" in df.columns and "quantity" in df.columns:
                df["total_amount"] = (df["unit_price"] * df["quantity"]).round(2)
            else:
                df["total_amount"] = 0.0

        completed = df[df["status"] == "COMPLETED"]
        if completed.empty:
            completed = df

        if "product_id" not in completed.columns:
            completed["product_id"] = "PROD-DEFAULT"
        if "quantity" not in completed.columns:
            completed["quantity"] = 1

        grp_cols = ["product_id"]
        if "category" in completed.columns:
            grp_cols.append("category")

        prod_agg = completed.groupby(grp_cols).agg(
            units_sold=("quantity", "sum"),
            gross_revenue=("total_amount", "sum"),
            orders_count=("order_id", "nunique"),
        ).reset_index()

        prod_agg["gross_revenue"] = prod_agg["gross_revenue"].round(2)

        # Enriquecimiento con costos de catálogo
        if df_products is not None and not df_products.empty:
            p_cols = ["product_id", "product_name", "cost_price"]
            avail_cols = [c for c in p_cols if c in df_products.columns]
            prod_agg = prod_agg.merge(df_products[avail_cols].drop_duplicates("product_id"), on="product_id", how="left")

        if "product_name" not in prod_agg.columns:
            prod_agg["product_name"] = prod_agg["product_id"]
        if "cost_price" not in prod_agg.columns or prod_agg["cost_price"].isnull().all():
            prod_agg["cost_price"] = (prod_agg["gross_revenue"] / prod_agg["units_sold"] * 0.6).round(2)
        else:
            prod_agg["cost_price"] = prod_agg["cost_price"].fillna(0.0).astype("float64")

        prod_agg["total_cost"] = (prod_agg["units_sold"] * prod_agg["cost_price"]).round(2)
        prod_agg["margin"] = (prod_agg["gross_revenue"] - prod_agg["total_cost"]).round(2)

        # Métricas de devoluciones por producto
        if df_returns is not None and not df_returns.empty and "product_id" in df_returns.columns:
            ret_prod = df_returns.groupby("product_id").agg(returns_count=("return_id", "count")).reset_index()
            prod_agg = prod_agg.merge(ret_prod, on="product_id", how="left")
            prod_agg["returns_count"] = prod_agg["returns_count"].fillna(0).astype("int32")
        else:
            prod_agg["returns_count"] = 0

        prod_agg["return_rate"] = (
            prod_agg["returns_count"] / prod_agg["units_sold"]
        ).fillna(0.0).round(4)

        # Clasificación por desempeño
        def classify_performance(row):
            if row["margin"] > 500 and row["return_rate"] <= 0.05:
                return "Top Performer"
            elif row["margin"] < 0 or row["return_rate"] > 0.15:
                return "Underperforming"
            else:
                return "Standard Performer"

        prod_agg["performance_tier"] = prod_agg.apply(classify_performance, axis=1)
        prod_agg["_gold_calculated_at"] = datetime.now(timezone.utc).isoformat()
        return prod_agg

    def process_silver_to_gold(
        self,
        silver_object_key: str,
        df_silver_override: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """Lee Parquet de Silver, construye los modelos Gold, los persiste en MinIO y sincroniza PostgreSQL."""
        silver_bucket = self.settings.get_bucket("silver")
        gold_bucket = self.settings.get_bucket("gold")

        logger.info(f"Procesando capa Gold a partir de Silver ({silver_bucket}/{silver_object_key})...")

        if df_silver_override is not None:
            df_silver = df_silver_override
        else:
            parquet_bytes = self.s3_client.read_bytes(silver_bucket, silver_object_key)
            df_silver = pd.read_parquet(io.BytesIO(parquet_bytes))

        if df_silver.empty:
            logger.warning("Dataset Silver vacío. No se generaron métricas Gold.")
            return {"status": "SKIPPED", "kpis_count": 0}

        # 1. Construir modelos analíticos Gold
        df_daily_sales = self.build_daily_sales(df_silver)
        df_customer_360 = self.build_customer_360(df_silver)
        df_product_perf = self.build_product_performance(df_silver)

        # 2. Resumen diario para compatibilidad con PostgreSQL analytics.daily_sales_kpis
        kpi_rows: List[Dict[str, Any]] = []
        grouped = df_silver.groupby("order_date")
        for order_date, group in grouped:
            total_orders = len(group)
            completed_group = group[group["status"] == "COMPLETED"]
            completed_orders = len(completed_group)
            cancelled_orders = len(group[group["status"] == "CANCELLED"])

            total_revenue = round(float(completed_group["total_amount"].sum()), 2)
            average_ticket = round(total_revenue / completed_orders, 2) if completed_orders > 0 else 0.00
            distinct_customers = int(group["customer_id"].nunique())

            top_category = "General"
            if not group.empty and "category" in group.columns:
                cat_counts = group.groupby("category")["total_amount"].sum()
                if not cat_counts.empty:
                    top_category = str(cat_counts.idxmax())

            kpi_rows.append({
                "kpi_date": order_date,
                "total_orders": total_orders,
                "completed_orders": completed_orders,
                "cancelled_orders": cancelled_orders,
                "total_revenue": total_revenue,
                "average_ticket": average_ticket,
                "distinct_customers": distinct_customers,
                "top_category": top_category,
            })

        # 3. Guardar Parquet en MinIO Gold para cada modelo
        now_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        def _save_parquet(df: pd.DataFrame, key: str) -> str:
            buf = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(df), buf, compression="SNAPPY")
            return self.s3_client.upload_bytes(buf.getvalue(), gold_bucket, key)

        gold_key = f"analytics/daily_sales/gold_daily_sales_{now_ts}.parquet"
        gold_uri = _save_parquet(df_daily_sales, gold_key)

        cust_key = f"analytics/customer_360/gold_customer_360_{now_ts}.parquet"
        cust_uri = _save_parquet(df_customer_360, cust_key)

        prod_key = f"analytics/product_performance/gold_product_performance_{now_ts}.parquet"
        prod_uri = _save_parquet(df_product_perf, prod_key)

        logger.info(f"Modelos Parquet Gold guardados exitosamente:")
        logger.info(f" - daily_sales: {gold_uri} ({len(df_daily_sales)} registros)")
        logger.info(f" - customer_360: {cust_uri} ({len(df_customer_360)} clientes)")
        logger.info(f" - product_performance: {prod_uri} ({len(df_product_perf)} productos)")

        # 4. Sincronización en PostgreSQL
        db_published_count = 0
        try:
            if self.db_client.ping():
                db_published_count = self.db_client.upsert_daily_sales_kpis(kpi_rows)
                if not df_customer_360.empty:
                    self.db_client.upsert_customer_360_kpis(df_customer_360.to_dict(orient="records"))
                if not df_product_perf.empty:
                    self.db_client.upsert_product_performance(df_product_perf.to_dict(orient="records"))
                logger.info(f"Métricas publicadas en PostgreSQL analytics ({db_published_count} registros diarios)")
            else:
                logger.warning("PostgreSQL analítico no disponible. Se continúa con MinIO Gold.")
        except Exception as e:
            logger.warning(f"No se pudo sincronizar en PostgreSQL: {e}")

        return {
            "status": "SUCCESS",
            "gold_uri": gold_uri,
            "gold_key": gold_key,
            "customer_360_uri": cust_uri,
            "product_performance_uri": prod_uri,
            "kpis_count": len(kpi_rows),
            "daily_sales_count": len(df_daily_sales),
            "customer_360_count": len(df_customer_360),
            "product_performance_count": len(df_product_perf),
            "db_published_count": db_published_count,
            "sample_kpi": kpi_rows[0] if kpi_rows else None,
        }
