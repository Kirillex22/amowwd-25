from data_pipeline.src.get_dataset import get_dataset
from data_pipeline.src.load_data_to_db import load_data_to_db
from data_pipeline.src.db import call_fn_etl_data_load
from sqlalchemy import text


def test_etl_end_to_end(engine):
    records = get_dataset(5)

    load_data_to_db(records)

    start = '2000-01-01'
    end = '2100-01-01'
    call_fn_etl_data_load(start, end)

    with engine.connect() as conn:
        count = conn.execute(text("select count(*) from s_sql_dds.t_sql_source_structured")).scalar()
        assert count == 5
