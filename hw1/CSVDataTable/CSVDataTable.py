import csv  # Python package for reading and writing CSV files.
import pandas as pd
import os

# You can change to wherever you want to place your CSV files.
rel_path = os.path.realpath('../Data')


class CSVDataTable:
    # Change to wherever you want to save the CSV files.
    data_dir = rel_path + "/"

    def __init__(self, table_name, table_file, key_columns):
        """
        Constructor
        :param table_name: Logical names for the data table.
        :param table_file: Name of CSV file to read/write(Location).
        :param key_columns: List of column names the form the primary key.
        """
        self.table_name = table_name;
        self.table_file = table_file;
        self.key_columns = key_columns;
        self.fieldnames = list();
        self.table_data = list();
        pass

    def __str__(self):
        """
        Pretty print the table and state.
        :return: String
        If we can use panda do as follows
        """;
        table_data = self.table_data;
        pd.set_option('display.max_columns', 500);
        pd.set_option('display.width', 1000);
        print(pd.DataFrame(table_data, columns=self.fieldnames));

        pass

    def load(self):
        """
        Load information from CSV file.
        :return: rows
        """
        self.table_data = [];
        with open(self.data_dir + self.table_file) as csvfile:
            rows = csv.DictReader(csvfile);
            self.fieldnames = rows.fieldnames;
            for row in rows:
                self.table_data.append(dict(row));
        return self.table_data;

    def find_by_primary_key(self, values, fields=None):
        """
        Input value is a list of string. The order of values should correspond to key_columns.
        Fields is a list defining which of the fields from the row/tuple you want.
        Output is the single dictionary in the table that is the matching result, or null/None.
        """
        result = list();
        if len(values) != len(self.key_columns):
            raise ValueError(str(self.key_columns), "Invalid values for primary keys");
        else:
            for row in self.table_data:
                flag = True;
                i = 0;
                for key in self.key_columns:
                    if row[key] != values[i]:
                        flag = False;
                    i = i + 1;
                if flag:
                    if fields is None:
                        result.append(row);
                    else:
                        new_row = dict();
                        for f in fields:
                            new_row[f] = row[f];
                        result.append(new_row);

            if len(result) > 1:
                raise ValueError(str(self.key_columns), "Invalid primary keys with multiple matching result")
            if len(result) < 1:
                print("There is no matching result for values: " + str(values));

        return result;

    def find_by_template(self, t, fields=None):
        """
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: CSVTable containing the answer.
        """
        rows = self.table_data;
        result = list();
        for row in rows:
            flag = True;
            for k in t.keys():
                if k in self.fieldnames:
                    if row[k] != t.get(k):
                        flag = False;
                else:
                    return KeyError(k, "Invalid key");
            if flag:
                if fields is None:
                    result.append(row);
                else:
                    new_row = dict();
                    for f in fields:
                        new_row[f] = row[f];
                    result.append(new_row);
        return result;

    def save(self):
        """
        Write updated CSV back to the original file location.
        :return: None
        """
        with open(self.data_dir + self.table_file, 'w') as csvTable:
            rows = self.table_data;
            writer = csv.DictWriter(csvTable, self.fieldnames);
            writer.writeheader();
            writer.writerows(rows);
        pass

    def insert(self, r):
        """
        Insert a new row into the table.
        :param r: New row. r is a dict of <fieldname, value>
        :return: None. Table state is updated.
        """
        row = dict();
        # check key validity
        for k in r.keys():
            if k not in self.fieldnames:
                raise KeyError(k, "Invalid key");
        # check primary key completeness
        pk_value = list();
        for pk in self.key_columns:
            if pk not in r.keys():
                raise KeyError(k, "Lack of primary key");
            else:
                pk_value.append(r.get(pk));
        # check duplicate primary key
        try:
            result = self.find_by_primary_key(pk_value, self.key_columns);
        except (ValueError, KeyError):
            pass

        if len(result) > 0:
            raise KeyError(k, "Duplicate primary key");

        for f in self.fieldnames:
            if f in r.keys():
                row[f] = r[f];
            else:
                row[f] = None;
        self.table_data.append(row);
        return self.table_data;

    def delete(self, t):
        """
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        """
        try:
            result = self.find_by_template(t);
        except (ValueError, KeyError) as e:
            print(e);
        if len(result) <= 0:
            print("Nothing to delete");
            pass

        for row in result:
            if row in self.table_data:
                self.table_data.remove(row);
        return self;
