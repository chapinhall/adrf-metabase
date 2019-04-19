"""Main script for extracting metadata from a table.

This script includes some set up to handle the data receipt part of the
metabase that has not be implemented yet including updating metabase.data_table
with a new data_table_id and the table name (including schema). The script then
extracts metadata from this table and updates metabase.column_info,
metabase.numeric_column, metabase.date_columns and metabase.code_frequency
as appropriate.

The new data_table_is displayed when the script is run. Following queries in
PostgreSQL will show the updates to the metabase:

select * from metabase.column_info where data_table_id = <data_table_id>;
select * from metabase.numeric_column where data_table_id = <data_table_id>;
select * from metabase.text_column where data_table_id =  <data_table_id>;
select * from metabase.date_column where data_table_id = <data_table_id>;
select * from metabase.code_frequency where data_table_id = <data_table_id>;

"""

import sys

import sqlalchemy

from metabase import extract_metadata
from metabase import parse_input


def update_data_table(full_table_name):
    """Update meatabase.data_table with this new table.

    This function is not intended to be part of the final metabase design but
    it is useful for testing in this stage.


    """

    engine = sqlalchemy.create_engine(
        'postgres://metaadmin@localhost/postgres')
    max_id = engine.execute(
        'SELECT MAX(data_table_id) FROM metabase.data_table'
    ).fetchall()[0][0]
    if max_id is None:
        new_id = 1
    else:
        new_id = max_id + 1
        print("data_table_id is {} for table {}".format(
            new_id, full_table_name))

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

    return new_id


if __name__ == "__main__":

    args = parse_input.parse_command_line_args(sys.argv[1:])
    full_table_name = parse_input.derive_full_table_name(args)
    categorical_threshold = args.categorical
    input_file = args.input_file
    type_overrides = {}

    if input_file is not None:
        file_parser = parse_input.ParseInput()
        file_parser.parse(input_file)
        type_overrides = file_parser.type_overrides
        categ_threshold_config = file_parser.categorical_threshold
        gmeta_output = file_parser.gmeta_output

    new_id = update_data_table(full_table_name)

    # Extract metadata from data.
    if categ_threshold_config:
        categorical_threshold = categ_threshold_config

    extract = extract_metadata.ExtractMetadata(data_table_id=new_id)

    extract.process_table(
        categorical_threshold=categorical_threshold,
        type_overrides=type_overrides)

    # Export metadata as Gmeta in JSON.
    if gmeta_output:
        extract.export_table_metadata(gmeta_output)
