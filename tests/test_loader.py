import sqlite3
import os

DB_PATH = os.path.join("tests", "data_test.db")


def count_rows(table):
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    finally:
        conn.close()


def test_customers_loaded():
    assert os.path.exists(DB_PATH), "Test DB not created"
    assert count_rows("work_orders") > 0


def test_orders_loaded():
    assert count_rows("service_po_detail") > 0
