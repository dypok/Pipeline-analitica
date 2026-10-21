import pytest
from app.backend.services.sql_guard import SQLGuard, SecurityException


def test_sql_guard_valid_select():
    sql = "SELECT order_date, total_revenue FROM v_daily_sales WHERE shipping_country = 'Colombia'"
    sanitized = SQLGuard.validate_and_sanitize(sql)
    assert "LIMIT 500" in sanitized
    assert "SELECT order_date" in sanitized


def test_sql_guard_with_custom_limit():
    sql = "SELECT * FROM v_daily_sales LIMIT 50"
    sanitized = SQLGuard.validate_and_sanitize(sql)
    assert "LIMIT 50" in sanitized


def test_sql_guard_clamps_excessive_limit():
    sql = "SELECT * FROM v_daily_sales LIMIT 5000"
    sanitized = SQLGuard.validate_and_sanitize(sql, max_limit=100)
    assert "LIMIT 100" in sanitized


def test_sql_guard_rejects_empty_query():
    with pytest.raises(SecurityException, match="no puede estar vacía"):
        SQLGuard.validate_and_sanitize("   ")


def test_sql_guard_rejects_drop_table():
    sql = "DROP TABLE v_daily_sales"
    with pytest.raises(SecurityException, match="Solo se admiten consultas de lectura"):
        SQLGuard.validate_and_sanitize(sql)


def test_sql_guard_rejects_delete():
    sql = "DELETE FROM v_customer_360 WHERE customer_id = 'CUST-0001'"
    with pytest.raises(SecurityException, match="Solo se admiten consultas de lectura"):
        SQLGuard.validate_and_sanitize(sql)


def test_sql_guard_rejects_insert():
    sql = "INSERT INTO v_daily_sales VALUES ('2026-10-19', 'Colombia', 'Ropa', 100, 1, 1, 100, 0, 0)"
    with pytest.raises(SecurityException, match="Solo se admiten consultas de lectura"):
        SQLGuard.validate_and_sanitize(sql)


def test_sql_guard_rejects_comment_injection():
    sql = "SELECT * FROM v_daily_sales; -- comentario malicioso"
    with pytest.raises(SecurityException, match="caracteres o comentarios prohibidos"):
        SQLGuard.validate_and_sanitize(sql)


def test_sql_guard_rejects_chained_commands():
    sql = "SELECT * FROM v_daily_sales; DROP TABLE v_product_performance"
    with pytest.raises(SecurityException, match="caracteres o comentarios prohibidos"):
        SQLGuard.validate_and_sanitize(sql)


def test_sql_guard_rejects_system_tables():
    sql = "SELECT * FROM duckdb_tables"
    with pytest.raises(SecurityException, match="Acceso denegado a tabla o función del sistema"):
        SQLGuard.validate_and_sanitize(sql)
