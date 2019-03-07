import os
import mysql.connector
import pandas as pd


class RDBDataTable:

    def __init__(self, database_name, table_name, key_columns):
        """
        Constructor
        :param table_name: Logical names for the data table.
        :param table_file: File name of CSV file to read/write.
        :param key_columns: List of column names the form the primary key.
        """
        self.database_name = database_name;
        self.table_name = table_name;
        self.key_columns = key_columns;
        self.cursor = None;
        self.cnx = None;

        pass

    def __str__(self):
        """
        Pretty print the table and state.
        :return: String
        """
        # get the header line
        query_header_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema=\'" \
                           + self.database_name + "\' AND table_name=\'" + self.table_name + "\';";
        self.cursor.execute(query_header_sql);
        header_columns = self.cursor.fetchall();
        header = [];
        for column in header_columns:
            header.append(column[0]);

        # get all the rows
        query_all_sql = "SELECT * FROM " + self.table_name + ";";
        self.cursor.execute(query_all_sql);
        results = self.cursor.fetchall();

        # pretty print
        pd.set_option('display.max_columns', 500);
        pd.set_option('display.width', 1000);
        print(pd.DataFrame(results, columns=header));
        pass

    def load(self):
        """
        Load information from CSV file.
        :return: None
        """
        # connect to mysql
        self.cnx = mysql.connector.connect(user='root', password='123456',
                                           host='127.0.0.1', port=3306,
                                           database=self.database_name);
        self.cursor = self.cnx.cursor();
        pass

    def find_by_primary_key(self, values, fields=None):
        """
        Input value is a list of string. The order of values should correspond to key_columns.
        Fields is a list defining which of the fields from the row/tuple you want.
        Output is the single dictionary in the table that is the matching result, or null/None.
        """
        if len(values) != len(self.key_columns):
            raise ValueError(values, "incompatible values");
        i = 0;
        t = dict();
        for key in self.key_columns:
            t[key] = values[i];
            i += 1;
        return self.find_by_template(t, fields);

    def find_by_template(self, t, fields=None):
        """
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: List containing the answer.
        """
        where = self.template2where(t);
        if fields is None:
            query_sql = "SELECT * FROM " + self.table_name + " " + where + ";";
        else:
            query_sql = "SELECT " + self.list2str(fields) + " FROM " + self.table_name + " " + where + ";";
        print("query sql :" + query_sql);
        self.cursor.execute(query_sql);
        results = self.cursor.fetchall();
        return list(results);

    def save(self):
        """
        Write updated CSV back to the original file location.
        :return: None
        """
        try:
            self.cnx.commit();
        except Exception:
            print("ERROR:" + Exception);
            self.cnx.rollback();
        pass

    def insert(self, r):
        """
        Insert a new row into the table.
        :param r: New row.
        :return: None. Table state is updated.
        """
        fields = self.list2str(r.keys());
        values = list();
        for key in r.keys():
            values.append("\"" + str(r[key]) + "\"");
        insert_sql = "INSERT INTO " + self.table_name + " (" + fields + ")" + " VALUES " + "(" + self.list2str(
            values) + ");"
        print("insert sql :" + insert_sql)
        self.cursor.execute(insert_sql);
        pass

    def delete(self, t):
        """
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        """
        delete_sql = "DELETE FROM " + self.table_name + " " + self.template2where(t) + ";";
        print("delete sql :" + delete_sql);
        self.cursor.execute(delete_sql);
        pass

    def end_connection(self):
        self.cursor.close();
        self.cnx.close();

    def template2where(self, t):
        s = ""
        print(t)
        for (k, v) in t.items():
            if s != "":
                s += " AND "
            s += k + "='" + v + "'"
        if s != "":
            s = "WHERE " + s;
        return s;

    def list2str(self, l):
        result = "";
        for s in l:
            result += str(s) + ",";
        return result[0:len(result) - 1];


if __name__ == "__main__":
    rdbDataTable = RDBDataTable("baseball", "test", ["playerID", "birthCity"]);
    rdbDataTable.load();
    # print(rdbDataTable.find_by_template({"birthCountry": "USA"}, ["playerID"]));
    # rdbDataTable.insert({"playerID": "sleif01", "birthYear": 1996, "birthMonth": 5, "birthDay": 16, "birthCountry": "China",
    #                    "birthCity": "Nanjing"});
    print(rdbDataTable.find_by_primary_key(["sleif01", "Nanjing"]));
    # rdbDataTable.__str__();
