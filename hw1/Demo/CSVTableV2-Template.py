import csv  # Python package for reading and writing CSV files.
import copy  # Copy data structures.

import sys, os

# You can change to wherever you want to place your CSV files.
rel_path = os.path.realpath('./Data')


class CSVTable():
    # Change to wherever you want to save the CSV files.
    data_dir = rel_path + "/"

    def __init__(self, table_name, table_file, key_columns):
        """
        Constructor
        :param table_name: Logical names for the data table.
        :param table_file: File name of CSV file to read/write.
        :param key_columns: List of column names the form the primary key.
        """
        pass

    def __str__(self):
        """
        Pretty print the table and state.
        :return: String
        """
        pass

    def load(self):
        """
        Load information from CSV file.
        :return: None
        """
        pass

    def find_by_template(self, t, fields=None):
        """
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: CSVTable containing the answer.
        """
        pass

    def save(self):
        """
        Write updated CSV back to the original file location.
        :return: None
        """

    def insert(self, r):
        """
        Insert a new row into the table.
        :param r: New row.
        :return: None. Table state is updated.
        """
        pass

    def delete(self, t):
        """
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        """
        pass

