from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_pipeline.src.config import DB_URL
import pandas as pd

engine = create_engine(DB_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)

def get_engine():
    return engine

def get_session():
    return SessionLocal()

def run_sql_script(conn, sql_text: str):
    conn.execute(text(sql_text))
    conn.commit()

def call_fn_etl_data_load(start_date: str, end_date: str):
    with engine.connect() as conn:
        conn.execute(text("select s_sql_dds.fn_etl_data_load(:s, :e)"), {"s": start_date, "e": end_date})
        conn.commit()

def call_fn_etl_data_load_test(start_date: str, end_date: str):
    with engine.connect() as conn:
        conn.execute(text("select s_sql_dds.fn_etl_data_load_test(:s, :e)"), {"s": start_date, "e": end_date})
        conn.commit()

def get_unstructured_data(limit: int = 10):
    session = get_session()
    try:
        result = session.execute(text(f"SELECT * FROM s_sql_dds.t_sql_source_unstructured LIMIT {limit}"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        total_count = session.execute(text("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_unstructured")).scalar()
        return df, total_count
    finally:
        session.close()

def get_structured_data(limit: int = 10):
    session = get_session()
    try:
        result = session.execute(text(f"SELECT * FROM s_sql_dds.t_sql_source_structured ORDER BY signup_date DESC LIMIT {limit}"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        total_count = session.execute(text("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured")).scalar()
        return df, total_count
    finally:
        session.close()
