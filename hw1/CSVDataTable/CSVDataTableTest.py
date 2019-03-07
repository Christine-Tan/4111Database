import CSVDataTable as CSVDataTable_Pkg
import pandas as pd


def test_load_and_print(test_name, table_name, table_file, key_columns):
    print("\n*******************************")
    print("Test name = ", test_name)
    print("*******************************")
    csv_test = CSVDataTable_Pkg.CSVDataTable(table_name, table_file, key_columns);
    print("Table name = ", csv_test.table_name)
    print("Table file = ", csv_test.table_file)
    print("Table keys = ", csv_test.key_columns)
    csv_test.load()
    csv_test.__str__();


def test_template(test_name, table_name, table_file, key_columns, template, fields=None):
    print("\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)
    print("Fields = ", fields)
    print("*******************************")

    try:
        csv_test = CSVDataTable_Pkg.CSVDataTable(table_name, table_file, key_columns)
        csv_test.load()
        # the basic information of table
        print("Table name = ", csv_test.table_name)
        print("Table file = ", csv_test.table_file)
        print("Table keys = ", csv_test.key_columns)

        r = csv_test.find_by_template(template, fields)

        print("*******************************")
        print("Result table:")
        pd.set_option('display.max_columns', 500);
        pd.set_option('display.width', 1000);
        if fields is None:
            print(pd.DataFrame(r, columns=csv_test.fieldnames));
        else:
            print(pd.DataFrame(r, columns=fields));

    except ValueError as ve:
        print("Exception: ", ve)
    except KeyError as ke:
        print("Exception: ", ke);


def test_primary_key(test_name, table_name, table_file, key_columns, values, fields=None):
    print("\n*******************************")
    print("Test name = ", test_name)
    print("Values = ", values)
    print("Fields = ", fields)
    print("*******************************")

    try:
        csv_test = CSVDataTable_Pkg.CSVDataTable(table_name, table_file, key_columns)
        csv_test.load()
        # the basic information of table
        print("Table name = ", csv_test.table_name)
        print("Table file = ", csv_test.table_file)
        print("Table keys = ", csv_test.key_columns)

        r = csv_test.find_by_primary_key(values, fields)

        print("*******************************")
        print("Result table:")
        pd.set_option('display.max_columns', 500);
        pd.set_option('display.width', 1000);
        if fields is None:
            print(pd.DataFrame(r, columns=csv_test.fieldnames));
        else:
            print(pd.DataFrame(r, columns=fields));

    except ValueError as ve:
        print("Exception: ", ve)
    except KeyError as ke:
        print("Exception: ", ke);


def test_insert(test_name, table_name, table_file, key_columns, row):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Row to insert = ", row)
    print("*******************************")

    try:
        csv_test = CSVDataTable_Pkg.CSVDataTable(table_name, table_file, key_columns)
        csv_test.load()
        print("Table name = ", csv_test.table_name)
        print("Table file = ", csv_test.table_file)
        print("Table keys = ", csv_test.key_columns)

        csv_test.insert(row)
        csv_test.save()
        print("Result table:")
        csv_test.__str__();

    except ValueError as ve:
        print("Exception: ", ve)
    except KeyError as ke:
        print("Exception: ", ke);


def test_delete(test_name, table_name, table_file, key_columns, t):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Delete template = ", t)
    print("*******************************")

    try:
        csv_test = CSVDataTable_Pkg.CSVDataTable(table_name, table_file, key_columns)
        csv_test.load()

        print("Table name = ", csv_test.table_name)
        print("Table file = ", csv_test.table_file)
        print("Table keys = ", csv_test.key_columns)
        csv_test.delete(t)
        csv_test.save()
        print("Result table:")
        csv_test.__str__();

    except ValueError as ve:
        print("Exception: ", ve.__str__())
    except KeyError as ke:
        print("Exception: ", ke.__str__());


if __name__ == "__main__":
    test_load_and_print("Load and print test.csv", "test", "test.csv", ["playerID"]);
    test_primary_key("Find by Primary Key-valid", "People", "PeopleSmall.csv", ["playerID"], ["aardsda01"]);
    test_primary_key("Find by Primary Key-no result", "People", "PeopleSmall.csv", ["playerID"], ["000"]);
    test_primary_key("Find by Primary Key-invalid value number", "People", "PeopleSmall.csv", ["playerID"], ["000", "lsief"]);
    test_template("Find by Template - valid", "People", "PeopleSmall.csv", ["playerID"], {"nameFirst":"David"});
    test_template("Find by Template - no result", "People", "PeopleSmall.csv", ["playerID"], {"playerID": "aardsda01","nameFirst":"Tiny"});
    test_template("Find by Template - no result", "People", "PeopleSmall.csv", ["playerID"], {"id": "aardsda01","nameFirst":"Tiny"});
    test_insert("Insert - valid", "People", "PeopleSmall.csv", ["playerID"],
                {"playerID": "xt2215", "birthYear": "1996", "birthMonth": "5", "birthDay": "16", "birthCountry": "China",
                 "birthCity": "Nanjing","nameFirst":"Christine","nameLast":"Tan"});
    test_insert("Insert - invalid", "People", "PeopleSmall.csv", ["playerID"],
                {"playerID": "xt2233", "birthYear": "1996", "birthMonth": "5", "birthDay": "16",
                 "birthCountry": "China",
                 "birthCity": "Nanjing", "nameFirst": "Christine", "nameLast": "Tan","CITY":"NONE"});
    test_delete("Delete - valid", "People", "PeopleSmall.csv", ["playerID"], {"nameFirst":"Christine"});
    test_delete("Delete - invalid", "People", "PeopleSmall.csv", ["playerID"], {"nameFirst":"Jenny"});
