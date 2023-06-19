import pandas as pd

import config
import psql

db = psql.HW1_db()

db.execute_sql('sql/create_schema_hw1.sql')
db.execute_sql('sql/create_table_hw1.okved.sql')

okved = pd.read_json(config.okved_filepath, compression='zip')
db.insert_values(okved, schema=config.schema, table=config.okved_table)
