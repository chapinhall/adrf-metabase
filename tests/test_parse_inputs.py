"""
Tests for parse_input.py

"""


import pytest

from metabase import parse_input

def test_parse():
    """ """

    parser = parse_input.ParseInput()
    parser.parse('tests/input_1.json')
