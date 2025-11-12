import os
import pytest
from sqlalchemy import create_engine, text
from data_pipeline.src.config import DB_URL
from data_pipeline import REPO_ROOT

@pytest.fixture(scope="session")
def engine():
    e = create_engine(DB_URL, future=True)
    return e

@pytest.fixture(scope="session", autouse=True)
def prepare_schema(engine):
    sql_dir = os.path.join(REPO_ROOT, "sql", "dds", "s_sql_dds")
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists s_sql_dds"))
        conn.execute(text(open(os.path.join(sql_dir, "function", "fn_etl_data_load.sql")).read()))
        conn.execute(text(open(os.path.join(sql_dir, "table", "t_sql_source_unstructured.sql")).read()))
        conn.execute(text(open(os.path.join(sql_dir, "table", "t_sql_source_structured.sql")).read()))
        conn.execute(text(open(os.path.join(sql_dir, "function", "fn_etl_data_load.sql")).read()))
    yield
    with engine.begin() as conn:
        conn.execute(text("drop schema if exists s_sql_dds cascade"))
