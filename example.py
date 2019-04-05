"""Example script for extracting metadata from a table.

This script requires ``pandas`` package being installed and ``metabase`` schema
being established. The latter can be done with ``alembic upgrade head``.

This script includes some set up to handle the data receipt part of the
metabase that has not be implemented yet including loading a csv file into a
table in PostgreSQL and updating metabase.data_table with a new data_table_id
and the table name (including schema). The script then extracts metadata from
this table and updates metabase.column_info, metabase.numeric_column,
metabase.date_columns and metabase.code_frequency as appropriate.

The new data_table_is displayed when the script is run. Following queries in
PostgreSQL will show the updates to the metabase:

select * from metabase.column_info where data_table_id = <data_table_id>;
select * from metabase.numeric_column where data_table_id = <data_table_id>;
select * from metabase.text_column where data_table_id =  <data_table_id>;
select * from metabase.date_column where data_table_id = <data_table_id>;
select * from metabase.code_frequency where data_table_id = <data_table_id>;

"""

import pandas as pd
import numpy as np
import sqlalchemy

from metabase import extract_metadata


############################################
# Change here.
############################################
file_name = 'data/adrf-000103.csv'
schema_name = 'data'    # Must specify a schema.
table_name = 'example'
categorical_threshold = 5
############################################

full_table_name = schema_name + '.' + table_name

# Create a text only table in the data base data.example.
data = pd.read_csv(file_name, encoding='latin-1')
# Convert nan to '' tempoarily for testing.
# TODO
data = data.replace(np.nan, '', regex=True)

engine = sqlalchemy.create_engine('postgres://metaadmin@localhost/postgres')
conn = engine.connect()
data.to_sql(
    table_name, conn,
    if_exists='replace',
    index=False,
    schema=schema_name)

# Update meatabase.data_table with this new table.
max_id = engine.execute(
    'SELECT MAX(data_table_id) FROM metabase.data_table'
    ).fetchall()[0][0]
if max_id is None:
    new_id = 1
else:
    new_id = max_id + 1
print("data_table_id is {} for table {}".format(new_id, full_table_name))

engine.execute(
    """
    INSERT INTO metabase.data_table
    (
    data_table_id,
    file_table_name
    )
    VALUES
    (
    %(data_table_id)s,
    %(file_table_name)s
    )
    """,
    {
        'data_table_id': new_id,
        'file_table_name': full_table_name
    }
)

# Extract metadata from data.
extract = extract_metadata.ExtractMetadata(data_table_id=new_id)
extract.process_table(categorical_threshold=categorical_threshold)
