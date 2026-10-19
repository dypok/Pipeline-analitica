"""Generador de dataset de prueba reproducible para Retail / E-commerce."""

import json
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple


class MockDataGenerator:
    """Genera datos sintéticos reproducibles con casos válidos y anomalías controladas."""

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.categories = ["Electronics", "Fashion", "Home & Kitchen", "Books", "Sports", "Beauty"]
        self.countries = ["CO", "MX", "AR", "CL", "PE", "US", "ES"]
        self.statuses = ["COMPLETED", "PENDING", "CANCELLED", "SHIPPED"]
        self.payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "PSE", "PAYPAL", "CASH"]

    def generate_customers(self, count: int = 100) -> List[Dict[str, Any]]:
        """Genera un listado de clientes sintéticos."""
        customers: List[Dict[str, Any]] = []
        first_names = ["Carlos", "Ana", "Luis", "Maria", "Juan", "Laura", "Diego", "Sofia", "Andres", "Valentina"]
        last_names = ["Gomez", "Rodriguez", "Perez", "Hernandez", "Martinez", "Garcia", "Torres", "Lopez"]

        base_date = datetime(2025, 1, 1)

        for i in range(1, count + 1):
            fn = random.choice(first_names)
            ln = random.choice(last_names)
            # 5% de anomalías en emails para probar cuarentena
            if i % 20 == 0:
                email = f"correo_invalido_{i}_sin_arroba.com"
            else:
                email = f"{fn.lower()}.{ln.lower()}{i}@example.com"

            cust_date = base_date + timedelta(days=random.randint(0, 365))
            customers.append(
                {
                    "customer_id": f"CUST-{i:05d}",
                    "first_name": fn,
                    "last_name": ln,
                    "email": email,
                    "country": random.choice(self.countries),
                    "registration_date": cust_date.strftime("%Y-%m-%d"),
                    "status": "ACTIVE" if random.random() > 0.1 else "INACTIVE",
                }
            )
        return customers

    def generate_orders(self, count: int = 500, customer_count: int = 100) -> List[Dict[str, Any]]:
        """Genera un listado de órdenes de compra con métricas e items."""
        orders: List[Dict[str, Any]] = []
        base_date = datetime(2026, 1, 1)

        for i in range(1, count + 1):
            cust_id = f"CUST-{random.randint(1, customer_count):05d}"
            order_date = base_date + timedelta(days=random.randint(0, 90))
            category = random.choice(self.categories)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5.0, 500.0), 2)
            total_amount = round(quantity * unit_price, 2)
            status = random.choice(self.statuses)

            # Inyectar anomalías deliberadas en el 5% de registros para validar Quarantine
            anomaly_type = None
            if i % 25 == 0:
                # Monto negativo inválido
                total_amount = -150.00
                anomaly_type = "NEGATIVE_AMOUNT"
            elif i % 40 == 0:
                # Estado desconocido inválido
                status = "UNKNOWN_CORRUPT"
                anomaly_type = "INVALID_STATUS"
            elif i % 70 == 0:
                # Falta customer_id o es nulo
                cust_id = ""
                anomaly_type = "MISSING_CUSTOMER_ID"

            orders.append(
                {
                    "order_id": f"ORD-{i:06d}",
                    "customer_id": cust_id,
                    "order_date": order_date.strftime("%Y-%m-%d"),
                    "category": category,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "status": status,
                    "payment_method": random.choice(self.payment_methods),
                    "_test_anomaly": anomaly_type,
                }
            )

        return orders

    def generate_dataset(self, num_customers: int = 50, num_orders: int = 250) -> Dict[str, Any]:
        """Genera el dataset completo de prueba con clientes y órdenes."""
        return {
            "metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "version": "1.0",
                "generator": "MockDataGenerator",
            },
            "customers": self.generate_customers(num_customers),
            "orders": self.generate_orders(num_orders, customer_count=num_customers),
        }
