"""
Tests for parse_input.py

"""

import collections
import pytest

from metabase import parse_input


def test_parse_file():
    """Test parsing an input json file."""

    parser = parse_input.ParseInput()
    parser.parse('tests/input_1.json')

    assert 'schema_1' == parser.schema
    assert 'table_1' == parser.table
    assert 10 == parser.categorical_trheshold
    assert 'YYYY-MM' == parser.date_format
    assert "text" == parser.type_overrides['col1']
    assert "categorical" == parser.type_overrides['col2']


def test_parse_command_line_args_table_schema():
    """Test parsing command line inputs table and schema."""

    args = ['-s', 'schema_1', '-t', 'table_1']

    parsed_args = parse_input.parse_command_line_args(args)

    assert 'schema_1' == parsed_args.schema
    assert 'table_1' == parsed_args.table


def test_parse_command_line_args_table_file():
    """Test parsing command line input input_file."""

    args = ['-f', 'my_file']

    parsed_args = parse_input.parse_command_line_args(args)

    assert 'my_file' == parsed_args.input_file


def test_parse_command_line_args_no_schema():
    """Test parsing invalid command line argugments."""

    args = ['-t', 'table_1']

    with pytest.raises(ValueError):
        parse_input.parse_command_line_args(args)


def test_parse_command_line_args_no_table():
    """Test parsing invalid command line argugments."""

    args = ['-s', 'schema_1']

    with pytest.raises(ValueError):
        parse_input.parse_command_line_args(args)


def test_parse_command_line_args_schema_and_file():
    """Test parsing invalid command line argugments."""

    args = ['-s', 'schema_1', '-f', 'my_file']

    with pytest.raises(ValueError):
        parse_input.parse_command_line_args(args)


def test_parse_command_line_args_table_and_file():
    """Test parsing invalid command line argugments."""

    args = ['-t', 'table_1', '-f', 'my_file']

    with pytest.raises(ValueError):
        parse_input.parse_command_line_args(args)


def test_derive_full_table_name_command_line():
    """Test derive full table name from command line style arguments."""

    args = collections.namedtuple('arg', ['schema', 'table', 'input_file'])
    a = args(schema='schema_1', table='table_1', input_file=None)
    full_table = parse_input.derive_full_table_name(a)

    assert 'schema_1.table_1' == full_table


def test_derive_full_table_name_command_file():
    """Test derive full table name from command input file."""

    args = collections.namedtuple('arg', ['schema', 'table', 'input_file'])
    a = args(schema=None, table=None, input_file='tests/input_1.json')
    full_table = parse_input.derive_full_table_name(a)

    assert 'schema_1.table_1' == full_table
