from data_pipeline.src.etl import etl
from data_pipeline.src.init_schema import init_schema

if __name__ == "__main__":
    print("Initializing schema...")
    init_schema()
    print("Running ETL...")
    etl()
