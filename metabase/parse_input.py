"""Class to parse json input."""

import json

class ParseInput():
    """Class to parse json input."""

    def __init__(self):

        self.schema = ''
        self.table = ''
        self.categorical_trheshold = ''
        self.date_format = ''
        self.type_overrides = ''

    def parse(self, file_name):
        """Load and parse input data in file_name.

        Args:
            file_name (str): json file containing input params

        """

        with open(file_name) as f:
            data = json.load(f)

        self.schema = data['schema']
        self.table = data['table']
        self.categorical_trheshold = data['categorical_threshold']
        self.date_format = data['date_format']
        self.type_overrides = data['type_overrides']




