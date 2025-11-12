from data_pipeline.src.get_dataset import get_dataset
from data_pipeline.src.load_data_to_db import load_data_to_db
from data_pipeline.src.fill_structured_table import fill_structured_table
from data_pipeline.src.db import get_unstructured_data, get_structured_data

def etl(count):
    print("starting etl...")
    records = get_dataset(n_rows=count, use_static_uuid=True)
    print(f"\n--- Сгенерированные записи ({len(records)}) ---")
    for record in records:
        print(record)
    load_data_to_db(records)
    
    # выводим сырые данные
    print("\n--- Сырые данные из t_sql_source_unstructured ---")
    df_raw, raw_count = get_unstructured_data(limit=count)
    print(df_raw.to_string())
    print(f"Всего сырых записей: {raw_count}")
    
    fill_structured_table()
    print("etl finished")
    
    # выводим обработанные данные
    print("\n--- Обработанные данные из t_sql_source_structured ---")
    df, total_count = get_structured_data(limit=count)
    print(df.to_string())
    print(f"\nВсего записей в таблице: {total_count}")
