import csv  # Python package for reading and writing CSV files.

# You MAY have to modify to match your project's structure.
from src import CSVCatalog as CSVCatalog, DataTableExceptions

max_rows_to_print = 10


class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog('localhost', 3306, 'my_catalog', 'dbuser', 'dbuser', debug_mode=None)

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog. You have to implement  the called methods below.
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = None
            self.__load__()  # Load rows from the CSV file.

            # Build indexes defined in the metadata. We do not implement insert(), update() or delete().
            # So we can build indexes on load.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return: TableDefinition
        """
        self.__description__ = self.__catalog__.get_table(self.__table_name__);

        table_definition = self.__description__;
        self.__file_name__ = table_definition.csv_f;
        column_names = list();
        for col in table_definition.column_definitions:
            column_names.append(col.column_name);
        self.__column_names__ = column_names;
        indexes = list();
        for index in table_definition.index_definitions:
            indexes.append(index);
        self.__indexes__ = indexes;
        indexes_selectivity = {};
        for index in table_definition.index_definitions:
            indexes_selectivity[index.index_name] = table_definition.get_index_selectivity(index.index_name);
        self.__indexes_selectivity__ = indexes_selectivity;
        return self.__description__;

    # Load from a file and creates the table and data.
    def __load__(self):

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __add_row__(self, row):
        if self.__rows__ is None:
            self.__rows__ = list();
        self.__rows__.append(row);

    def __get_file_name__(self):
        return self.__file_name__;

    def __get_column_names__(self):
        return self.__column_names__;

    def __get_row_list__(self):
        return self.__rows__;

    def __get_index_by_name__(self, index_name):
        for index in self.__indexes__:
            if index.index_name == index_name:
                return index;
        return None;

    def __str__(self):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        info = "table_name: " + self.__table_name__ + "\n";
        info += "file_name" + self.__file_name__ + "\n";
        info += "Row count " + str(len(self.__rows__)) + "\n";
        info += "column_names: ";
        for column_name in self.__column_names__:
            info += column_name + " ";
        info += "\n" + "index_names:";
        for index in self.__indexes__:
            info += index.index_name + " ";
        info += "\n"
        return info;

    def __build_indexes__(self):
        indexes = self.__indexes__;
        file_name = self.__file_name__;
        # store the hash indexes in a dictionary
        # key is the index name
        # value is the hash index
        indexes_dict = dict();

        for row in self.__get_row_list__():
            for index in indexes:
                # for each index it has a hash index dictionary
                index_name = index.index_name;
                if index_name not in indexes_dict:
                    indexes_dict[index_name] = dict();

                index_columns = indexes_dict[index_name];

                # generate the key for the hash index
                index_column = list();
                for column in index.columns:
                    index_column.append(row[column]);
                index_column = tuple(index_column);

                # build each index into a hash index with a dictionary
                # key is the indexed column string tuple, value is a list of mapped rows
                if index_column not in index_columns:
                    index_columns[index_column] = list();

                index_columns[index_column].append(row)

        self.__indexes_dict__ = indexes_dict;
        return indexes_dict;

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.

        Best is defined as the most selective index, i.e. the one with the most distinct index entries.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: Query template.
        :return: Index or None
        """
        candidate_indexes = list();
        c_names = list(tmp.keys());
        for index in self.__indexes__:
            col_names = index.columns;
            flag = True;
            for col_name in col_names:
                if col_name not in c_names:
                    flag = False;
                    break;
            if flag:
                candidate_indexes.append(index);

        # get the most selective index
        most_selective_index = None;
        most_selectivity = 0;
        for index in candidate_indexes:
            selectivity = self.__indexes_selectivity__[index.index_name];
            if selectivity >= most_selectivity:
                most_selectivity = selectivity;
                most_selective_index = index;
        return most_selective_index;

    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:

            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # find the index
        index_dict = self.__indexes_dict__[idx]
        # generate the key for the hash index
        index = self.__get_index_by_name__(idx)
        if index is None:
            raise ("No such Index %s" % idx)

        index_columns = index.columns;
        t_columns = [];
        for index_column in index_columns:
            t_columns.append(t[index_column]);
        t_columns = tuple(t_columns);

        # matching tuples
        result_rows = list();
        if t_columns in index_dict:
            index_rows = index_dict[t_columns];

            for index_row in index_rows:
                if self.matches_template(index_row, t):
                    result_rows.append(index_row);

        self.project(result_rows, fields);
        return result_rows;

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        index = self.__get_access_path__(t);
        if index is not None:
            return self.__find_by_template_index__(t, index.index_name, fields, limit, offset);
        else:
            return self.__find_by_template_scan__(t, fields, limit, offset);

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )

    def nested_loop_join(self, right_r, on_fields, where_template=None, project_fields=None):
        scan_rows = self.__get_row_list__();
        join_result = [];

        # compute the JOIN based on the on_fields
        processed_rows = 0;
        for l_r in scan_rows:
            if processed_rows % 5000 == 0:
                print("processed rows %d" % processed_rows)
            on_template = self.__get_on_template__(l_r, on_fields);
            current_right_rows = right_r.find_by_template(on_template);

            if current_right_rows is not None and len(current_right_rows) > 0:
                # Merge the l_r row dictionaries into a single dictionary with each right row
                new_rows = self.__join_rows__([l_r], current_right_rows, on_fields);
                join_result.extend(new_rows);
            processed_rows += 1;

        final_rows = [];
        if where_template is not None and len(where_template) > 0:
            # compute SELECT based on the where_template and project_fields
            for r in join_result:
                if self.matches_template(r, where_template):
                    r = self.project([r], fields=project_fields);
                    final_rows.append(r[0]);
        else:
            final_rows = self.project(join_result, fields=project_fields);
        return final_rows;

    def optimized_join(self, right_r, on_fields, where_template=None, project_fields=None):
        join_result = [];
        print("optimize way 2: first apply select on two tables")
        if where_template is not None and len(where_template) > 0:
            print("optimize way 3: if there is an index, use it")

            left_rows = self.find_by_template(where_template);
            right_rows = right_r.find_by_template(where_template);

            for l_r in left_rows:
                on_template = self.__get_on_template__(l_r, on_fields);
                current_right_rows = [];
                for r in right_rows:
                    if self.matches_template(r, on_template):
                        current_right_rows.append(r);

                if current_right_rows is not None and len(current_right_rows) > 0:
                    new_rows = self.__join_rows__(left_rows, right_rows, on_fields);
                    join_result.extend(new_rows);

            # compute SELECT based on project_fields
            final_rows = self.project(join_result, fields=project_fields);
            return final_rows;
        else:
            return self.nested_loop_join(right_r, on_fields, where_template, project_fields);

    def join(self, right_r, on_fields, where_template=None, project_fields=None, optimize=False):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """

        # If not optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        if optimize:
            print("Optimizing!!!")
            # SWAP TABLES
            # if there is an index on the on_fields in the left table but not on the right table, swap the two tables
            index_l = self.__get_access_path__(dict((field, 0) for field in on_fields));
            index_r = right_r.__get_access_path__(dict((field, 0) for field in on_fields));
            print(index_l.to_json())
            # print(index_r.to_json())

            if (index_l is None) and (index_r is None):
                print("No available index to use! No optimization is possible, so Just a nested loop join!")
                return self.nested_loop_join(right_r, on_fields, where_template, project_fields);
            else:
                if index_r is None:
                    print("optimize way 1: swap the two joined tables")
                    return right_r.optimized_join(self, on_fields, where_template, project_fields);
                else:
                    return self.optimized_join(right_r, on_fields, where_template, project_fields);
        else:
            return self.nested_loop_join(right_r, on_fields, where_template, project_fields);

    def __join_rows__(self, left_rows, right_rows, on_fields):
        new_rows = list();
        for left_row in left_rows:
            for right_row in right_rows:
                new_row = dict();
                for left_field in left_row:
                    new_row[left_field] = left_row[left_field];
                for right_field in right_row:
                    if right_field not in on_fields:
                        new_row[right_field] = right_row[right_field];
                new_rows.append(new_row);
        return new_rows;

    def __get_on_template__(self, l_r, on_fields):
        tmp = dict();
        for field in on_fields:
            tmp[field] = l_r[field];
        return tmp;

    def __select_on_table__(self, where_template):
        col = self.__column_names__;
        new_tmp = dict()
        for tmp in where_template:
            if tmp in col:
                new_tmp[tmp] = where_template[tmp];
        cur_rows = self.find_by_template(new_tmp);
        return cur_rows;
