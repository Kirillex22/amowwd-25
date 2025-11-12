import os
from pathlib import Path
from sqlalchemy import text
from data_pipeline.src.db import get_engine
from data_pipeline import REPO_ROOT

def init_schema():
    engine = get_engine()

    sql_dir = REPO_ROOT / "sql" / "dds" / "s_sql_dds"
    
    ddl_order = [
        sql_dir / "table" / "t_sql_source_unstructured.sql",
        sql_dir / "table" / "t_sql_source_structured.sql",
        sql_dir / "table" / "t_sql_source_structured_copy.sql",
        sql_dir / "function" / "fn_etl_data_load.sql",
        sql_dir / "function" / "fn_etl_data_load_test.sql",
    ]
    
    with engine.begin() as conn:
        for sql_file in ddl_order:
            if sql_file.exists():
                sql_content = sql_file.read_text(encoding="utf-8")
                conn.execute(text(sql_content))
                print(f"Executed: {sql_file.name}")
            else:
                print(f"Warning: SQL file not found: {sql_file}")
