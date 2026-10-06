import sys
import os
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from producer.generator import generate_order

def test_generate_order():
    order = generate_order(1001)
    
    assert "order_id" in order
    assert order["order_id"] == 1001
    assert "customer_id" in order
    assert 1 <= order["customer_id"] <= 100
    assert "product" in order
    assert "category" in order
    assert "quantity" in order
    assert isinstance(order["quantity"], int)
    assert 1 <= order["quantity"] <= 5
    
    assert "unit_price" in order
    assert isinstance(order["unit_price"], float)
    assert order["unit_price"] > 0
    
    assert "city" in order
    assert isinstance(order["city"], str)
    
    assert "timestamp" in order
    assert isinstance(order["timestamp"], str)
