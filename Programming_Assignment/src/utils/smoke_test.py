# src/utils/smoke_test.py
from src.utils.config import PostgresConfig, MongoConfig, get_paths

if __name__ == "__main__":
    paths = get_paths()
    print("PROJECT_ROOT:", paths["root"])
    print("RAW:", paths["data_raw"])
    print("PROCESSED:", paths["data_processed"])

    pg = PostgresConfig()
    mg = MongoConfig()
    print("PG:", pg)
    print("MONGO:", mg)