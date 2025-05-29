from enum import Enum

class DBConnection(Enum):
    SOURCE = "postgres_db_src"
    STAGING = "postgres_db_stg"
    TARGET = "postgres_db_tgt"
