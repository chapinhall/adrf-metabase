"""Functions and classes to parse inputs including command line and files."""

import argparse
import json


class ParseInput():
    """Class to parse json input."""

    def __init__(self):

        self.schema = ''
        self.table = ''
        self.categorical_threshold = ''
        self.date_format = ''
        self.type_overrides = ''

    def parse(self, file_name):
        """Load and parse input data in file_name.

        Args:
            file_name (str): json file containing input params

        Retruns:
            (argparse.Namespace): Parsed arguments from argparse

        """

        with open(file_name) as f:
            data = json.load(f)

        self.schema = data['schema']
        self.table = data['table']
        self.categorical_threshold = data['categorical_threshold']
        self.date_format = data['date_format']
        self.type_overrides = data['type_overrides']


def parse_command_line_args(args):
    """Parse command line arguments.

    Args:
        args ([str]): List of command line arguments and flags.

    Returns
        (argparse.Namespace): Parsed arguments from argparse

    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-s', '--schema', type=str,
        help='Schema name of the data')
    parser.add_argument(
        '-t', '--table', type=str,
        help='Table name of the data')
    parser.add_argument(
        '-c', '--categorical', type=int, default=10,
        help='Max number of distinct values in all categorical columns')
    parser.add_argument(
        '-f', '--input_file', type=str,
        help='JSON file containing input parameters')

    out = parser.parse_args(args)

    # Validation
    msg = ('Either an input file or both a table name and schema name must be '
           'provided.')
    if out.input_file is None:
        if (out.schema is None) or (out.table is None):
            raise ValueError(msg)

    if out.input_file is not None:
        if (out.schema is not None) or (out.table is not None):
            raise ValueError(msg)

    return out


def derive_full_table_name(args):
    """Derive the full table name from command line arguments.

    Args:
        args (argparse.Namespace): Parsed arguments from argparse

    Returns (str) full table name including schema.

    """

    schema_name = args.schema
    table_name = args.table
    input_file = args.input_file

    full_table_name = ''
    if (schema_name is not None) and (table_name is not None):
        full_table_name = schema_name + '.' + table_name
    elif input_file is not None:
        file_parser = ParseInput()
        file_parser.parse(input_file)
        full_table_name = file_parser.schema + '.' + file_parser.table

    if full_table_name == '':
        raise ValueError('Invalid table and schema names provided.')

    return full_table_name
