from sqlalchemy import create_engine, text
import datetime

def test_fn_etl_data_load_test(engine):
    with engine.connect() as conn:
        conn.execute(text("truncate table s_sql_dds.t_sql_source_unstructured cascade"))
        conn.execute(text("truncate table s_sql_dds.t_sql_source_structured cascade"))

        conn.execute(text("""
        insert into s_sql_dds.t_sql_source_unstructured(id, name, age, category, value, country, city, signup_date, email) values
        ('1', '', -5, 'a', -10, 'russia', 'moscow', '2023-01-01', 'bad_email'),
        ('2', 'ivan', 25, 'b', 100.5, null, null, '2024-06-01', 'ivan@example.com'),
        ('3', null, 999, 'c', null, 'germany', 'berlin', '2024-01-15', null);
        """))
        conn.commit()

        conn.execute(text("select s_sql_dds.fn_etl_data_load('2022-01-01', '2025-12-31')"))
        conn.commit()

        res = conn.execute(text("select id, name, age, value, email from s_sql_dds.t_sql_source_structured order by id")).fetchall()
        r1 = res[0]
        assert r1[1] == 'unknown'
        assert r1[2] is None
        assert r1[3] is None
        assert r1[4] == 'unknown'

        r2 = res[1]
        assert r2[1] == 'ivan'
        assert r2[2] == 25
        assert float(r2[3]) == 100.5
        assert r2[4] == 'ivan@example.com'

        r3 = res[2]
        assert r3[2] is None
        assert r3[4] == 'unknown'
