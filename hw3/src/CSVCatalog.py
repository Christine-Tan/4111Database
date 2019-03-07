import pymysql
import json
import csv
import pandas as pd

index_table_name = "IndexDefinition"
table_table_name = "TableDefinition"
column_table_name = "ColumnDefinition"


class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """

        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        if column_name is not None and column_type in ColumnDefinition.column_types:
            self.column_name = column_name;
            self.column_type = column_type;
            self.not_null = not_null;
        else:
            raise Exception("Invalid column definition for %s, %s" % column_name % column_type);

    def __str__(self):
        return str(self)

    def to_json(self):
        """

        :return: A JSON object, not a string, representing the column and it's properties.
        """
        return json.dumps(self, default=lambda o: o.__dict__, indent=2);


class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, columns, index_type):
        """
        :param index_name: Name for index. Must be unique name for table.
        :param columns: column names that constitute the index
        :param index_type: Valid index type.
        """
        if index_type in self.index_types:
            self.index_name = index_name
            self.columns = columns
            self.index_type = index_type
        else:
            raise Exception("Invalid index type %s!" % index_type)

    def to_json(self):
        """
        :return: A JSON object, not a string, representing the column and it's properties.
        """
        return json.dumps(self, default=lambda o: o.__dict__, indent=2);


class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None):
        """
        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        if cnx is None:
            cnx = pymysql.connect(host='localhost',
                                  user='dbuser',
                                  password='dbuser',
                                  db='my_catalog',
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)
        self.cnx = cnx;

        self.table_name = t_name;
        self.csv_f = csv_f;

        self.column_definitions = list();
        if column_definitions is not None and len(column_definitions) > 0:
            for column_definition in column_definitions:
                # exists in the csv file
                if self.exist_column(column_definition.column_name):
                    # not duplicate
                    for column in self.column_definitions:
                        if column.column_name == column_definition.column_name:
                            raise Exception("Duplicate column %s" % column_definition.column_name);
                else:
                    raise Exception("Invalid column %s" % column_definition.column_name);
                # valid column names
                self.column_definitions.append(column_definition);

        self.index_definitions = list();
        if index_definitions is not None and len(index_definitions) > 0:
            for index_definition in index_definitions:
                for column in index_definition.columns:
                    if self.is_valid_column(column) is None:
                        raise Exception("Invalid column %s" % column)
                # valid column names
                self.index_definitions.append(index_definition);

    def __str__(self):
        return self.to_json();

    @classmethod
    def load_table_definition(cls, cnx, table_name):
        """
        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        select_table_q = get_select_table_sql(table_name);
        table_result = run_q(select_table_q, cnx, True);
        if table_result is not None and len(table_result) > 0:
            csv_f = table_result[0]["fileName"]
            select_column_q = get_select_column_sql(table_name);
            column_result = run_q(select_column_q, cnx, True);
            cols = []
            for column in column_result:
                cols.append(ColumnDefinition(column["columnName"], column["columnType"], column["notNull"]));

            select_index_q = get_select_index_sql(table_name);
            index_result = run_q(select_index_q, cnx, True);

            indexes = dict();
            for index in index_result:
                index_name = index["indexName"];
                if index_name not in indexes:
                    indexes[index_name] = list();
                    indexes[index_name].append(list());
                    indexes[index_name].append(index["indexType"]);
                indexes[index_name][0].append(index["columnName"]);

            idx = []
            for index_name in indexes:
                columns = indexes[index_name][0];
                index_type = indexes[index_name][1];
                idx.append(IndexDefinition(index_name, columns, index_type));
            return TableDefinition(table_name, csv_f, cols, idx, cnx);
        else:
            raise Exception("Table %s does not exist" % table_name);

    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        # exists in the csv file
        if self.exist_column(c.column_name):
            # not duplicate
            for column in self.column_definitions:
                if column.column_name == c.column_name:
                    raise Exception("Duplicate column %s" % c.column_name);
        else:
            raise Exception("Invalid column %s" % c.column_name);

        # valid: add column definition
        self.column_definitions.append(c);

        # add column to table ColumnDefinition in the database
        q = get_insert_column_sql(self.table_name, c);
        run_q(q, self.cnx, False)
        return self.column_definitions;

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        col = self.is_valid_column(c)
        if col is not None:
            self.column_definitions.remove(col);
            # remove this column from table in the database
            q = get_delete_column_sql(self.table_name, c);
            run_q(q, self.cnx, False)
        return self.column_definitions;

    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column names in order.
        :return:
        """
        index_name = "";
        for column in columns:
            index_name += column + "_";
        index_name = index_name[0:len(index_name) - 1];
        self.define_index(index_name, columns, kind="PRIMARY");
        return self.index_definitions;

    def define_index(self, index_name, columns, kind="INDEX"):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of column names.
        :param kind: One of the valid index types.
        :return:
        """
        # valid list of columns
        for column in columns:
            col = self.is_valid_column(column)
            if col is None:
                raise Exception("Invalid column %s" % column)

        # if there is primary key already, replace it
        if kind == "PRIMARY":
            # remove original one
            ori_primary = None;
            for index in self.index_definitions:
                if index.index_type == "PRIMARY":
                    ori_primary = index;
            if ori_primary is not None:
                # delete
                self.index_definitions.remove(ori_primary);
                q = get_delete_index_sql(self.table_name, ori_primary.index_name)
                run_q(q, self.cnx, False)

        # Ensure unique index name for table
        for index in self.index_definitions:
            if index.index_name == index_name:
                raise Exception("Duplicate index %s" % index_name)

        index = IndexDefinition(index_name, columns, kind)

        self.index_definitions.append(index)
        # add index into table IndexDefinition
        q = get_insert_index_sql(self.table_name, index);
        run_q(q, self.cnx, False)
        return self.index_definitions;

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        for index in self.index_definitions:
            if index.index_name == index_name:
                ind = index;
        if ind is not None:
            self.index_definitions.remove(ind);
            # drop index from table IndexDefinition
            q = get_delete_index_sql(self.table_name, index_name);
            run_q(q, self.cnx, False)
        return self.index_definitions;

    def get_index_selectivity(self, index_name):
        """
        Selectivity is N/M, where N is the number of tuples in a file
        M is the number of distinct values for this index.
        The less N/M means the more usefulness of an index.
        If the number of distinct values for this index is higher, it means that
        the index is more selective as the tuples in a file is a constant.

        :param index_name: should be something like colname1_colname2_colname3...
        :return:
        """
        csv_f = self.csv_f;
        # get the index by index_name
        idx = self.get_index_by_name(index_name)
        if idx is None:
            raise ("No such index %s" % index_name);
        column_names = idx.columns;
        rows = set();
        with open(csv_f, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                sp_row = "";
                for column_name in column_names:
                    sp_row += row[column_name] + "&";
                sp_row = sp_row[0:len(sp_row) - 1];
                rows.add(sp_row);
        return len(rows);

    def get_index_by_name(self,index_name):
        for index in self.index_definitions:
            if index.index_name == index_name:
                return index;
        return None;

    def exist_column(self, c):
        # valid column: exist in the csv file and not duplicate in table
        file = open(self.csv_f);
        l = file.readline();
        columns = l.split(",");
        if c not in columns:
            raise Exception("Column %s does not exist" % c);
        file.close();
        return True;

    def is_valid_column(self, c):
        for column in self.column_definitions:
            if column.column_name == c:
                return column;
        return None;

    def to_json(self):
        """

        :return: A JSON representation of the table and it's elements.
        """
        json_str = dict();
        json_str["table_name"] = self.table_name;
        json_str["table_file"] = self.csv_f;
        json_str["column_definitions"] = self.column_definitions;
        if self.index_definitions is not None and len(self.index_definitions) > 0:
            json_str["index_definitions"] = self.index_definitions;
        return json.dumps(json_str, default=lambda o: o.__dict__, indent=2);

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        return self.to_json()


class CSVCatalog:

    def __init__(self, dbhost, dbport, dbname, dbuser, dbpw, debug_mode=None):
        self.db_host = dbhost;
        self.db_port = dbport;
        self.db_name = dbname;
        self.db_user = dbuser;
        self.db_pw = dbpw;
        self.debug_mode = debug_mode;
        self.table_definitions = list();
        self.cnx = pymysql.connect(host=self.db_host,
                                   port=self.db_port,
                                   user=self.db_user,
                                   password=self.db_pw,
                                   db=self.db_name,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

    def __str__(self):
        return str(self);

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        index_name = "primary_key";
        index_type = "PRIMARY";
        index_list = list();
        if primary_key_columns is not None:
            index = IndexDefinition(index_name, primary_key_columns, index_type)
            index_list.append(index);

        table = TableDefinition(table_name, file_name, column_definitions, index_list, self.cnx);
        # add table definition to catalog
        self.table_definitions.append(table);
        # add table definition to table CSVCatalog
        q = get_insert_table_sql(table);
        run_q(q, self.cnx, False);
        # add column definition to ColumnDefinition
        if table.column_definitions is not None and len(table.column_definitions) > 0:
            q = get_insert_columns_sql(table);
            run_q(q, self.cnx, False);
        # add index definition to IndexDefinition
        if table.index_definitions is not None and len(table.index_definitions) > 0:
            q = get_insert_indexes_sql(table);
            run_q(q, self.cnx, False);

        return table;

    def drop_table(self, table_name):
        tab = None;
        for table in self.table_definitions:
            if table_name == table.table_name:
                tab = table;
        if tab is not None:
            self.table_definitions.remove(tab);

        # delete table from table CSVCatalog in mysql
        select_sql = get_select_table_sql(table_name);
        result = run_q(select_sql, self.cnx, True);
        if result is not None and len(result) > 0:
            q = get_delete_table_sql(table_name);
            run_q(q, self.cnx, False)
        return self.table_definitions;

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return: TableDefinition
        """
        return TableDefinition.load_table_definition(self.cnx, table_name);


def get_select_table_sql(table_name):
    table = table_table_name;
    q = "SELECT tableName, fileName FROM " + table + " WHERE tableName='" + table_name + "';";
    return q;


def get_select_column_sql(table_name):
    table = column_table_name;
    q = "SELECT columnName, columnType, notNull FROM " + table + " WHERE tableName = '" + table_name + "';";
    return q;


def get_select_index_sql(table_name):
    table = index_table_name;
    q = "SELECT indexName, indexType, columnName FROM " + table + " WHERE tableName = '" + table_name + "';";
    return q;


def get_insert_table_sql(table):
    table_name = table_table_name;
    value = list();
    value.append(table.table_name);
    value.append(table.csv_f);
    values = list2str(value)
    q = "INSERT INTO " + table_name + " (tableName, fileName ) VALUES " + values + ";";
    return q;


def get_delete_table_sql(table_name):
    table = table_table_name;
    q = "DELETE FROM " + table + " WHERE tableName='" + table_name + "';";
    return q;


def get_insert_columns_sql(table):
    table_name = column_table_name;
    values = "";
    for column in table.column_definitions:
        value = list();
        value.append(table.table_name);
        value.append(column.column_name);
        value.append(column.column_type);
        value.append(column.not_null);
        s = list2str(value) + ",";
        values += s;
    values = values[0:len(values) - 1];
    q = "INSERT INTO " + table_name + " (tableName, columnName, columnType, notNull ) VALUES " + values + ";";
    return q;


def get_insert_column_sql(table_name, column):
    table = column_table_name;
    value = list();
    value.append(table_name);
    value.append(column.column_name);
    value.append(column.column_type);
    value.append(column.not_null);
    values = list2str(value);
    q = "INSERT INTO " + table + " (tableName, columnName, columnType, notNull) VALUES " + values + ";";
    return q;


def get_delete_column_sql(table_name, column_name):
    table = column_table_name;
    q = "DELETE FROM " + table + " WHERE tableName='" + table_name + "' AND columnName='" + column_name + "';";
    return q;


def get_insert_indexes_sql(table):
    table_name = index_table_name;
    values = "";
    for index in table.index_definitions:
        pos = 1;
        value = list();
        for column in index.columns:
            value.append(table.table_name);
            value.append(index.index_name);
            value.append(index.index_type);
            value.append(column.column_name);
            value.append(pos);
            s = list2str(value) + ",";
            values += s;
            pos += 1;
    values = values[0:len(values) - 1];
    q = "INSERT INTO " + table_name + " (tableName, indexName, indexType, columnName, position) VALUES " + values + ";";
    return q;


def get_insert_index_sql(table_name, index):
    table = index_table_name;
    values = "";
    pos = 1;
    for column in index.columns:
        value = list();
        value.append(table_name);
        value.append(index.index_name);
        value.append(index.index_type);
        value.append(column);
        value.append(pos);
        s = list2str(value) + ",";
        values += s;
        pos += 1;

    values = values[0:len(values) - 1];
    q = "INSERT INTO " + table + " (tableName, indexName, indexType, columnName, position) VALUES " + values + ";";
    return q;


def get_delete_index_sql(table_name, index_name):
    table = index_table_name;
    q = "DELETE FROM " + table + " WHERE tableName='" + table_name + "' AND indexName='" + index_name + "';";
    return q;


def list2str(lists):
    if len(lists) > 0:
        s = "(";
        for l in lists:
            s += "'" + str(l) + "'" + ",";

        s = s[0: len(s) - 1] + ")";
        return s;
    return None;


def run_q(q, cnx, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result;
