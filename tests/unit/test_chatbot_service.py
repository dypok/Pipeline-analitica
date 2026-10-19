from app.backend.services.chatbot_service import ChatbotService


def test_chatbot_sales_question():
    service = ChatbotService()
    res = service.ask("¿Cuánto vendimos en total?")
    assert res["success"] is True
    assert res["sql"] is not None
    assert "total_revenue" in res["sql"].lower()
    assert "$" in res["answer"] or "ventas" in res["answer"].lower()
    assert res["row_count"] >= 1


def test_chatbot_country_question():
    service = ChatbotService()
    res = service.ask("¿Qué país generó más ingresos?")
    assert res["success"] is True
    assert res["sql"] is not None
    assert "shipping_country" in res["sql"].lower()
    assert len(res["data"]) > 0


def test_chatbot_top_products_question():
    service = ChatbotService()
    res = service.ask("¿Cuáles son los 5 productos más vendidos?")
    assert res["success"] is True
    assert "v_product_performance" in res["sql"]
    assert len(res["data"]) <= 5


def test_chatbot_impossible_question_p09():
    service = ChatbotService()
    # Prueba P09: Pregunta sobre el futuro lejano o imposible
    res = service.ask("¿Cuál será la proyección de ventas para el año 2045?")
    assert res["success"] is True
    assert res["sql"] is None
    assert "históricos" in res["answer"].lower()
    assert "no es posible" in res["answer"].lower()
    assert len(res["data"]) == 0
