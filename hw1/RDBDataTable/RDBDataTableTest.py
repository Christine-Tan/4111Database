import RDBDataTable as RDBDataTable_Pkg
import pandas as pd


def test_load_and_print(test_name, database_name, table_name, key_columns):
    print("\n*******************************")
    print("Test name = ", test_name)
    print("*******************************")
    rdb_test = RDBDataTable_Pkg.RDBDataTable(database_name, table_name, key_columns);
    rdb_test.load();
    print("Database name = ", rdb_test.database_name)
    print("Table name = ", rdb_test.table_name)
    print("Primary keys = ", rdb_test.key_columns)
    rdb_test.__str__();


def test_template(test_name, database_name, table_name, key_columns, template, fields=None):
    print("\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)
    print("Fields = ", fields)
    print("*******************************")

    rdb_test = RDBDataTable_Pkg.RDBDataTable(database_name, table_name, key_columns)
    rdb_test.load();
    # the basic information of table
    print("Database name = ", rdb_test.database_name)
    print("Table name = ", rdb_test.table_name)
    print("Primary keys = ", rdb_test.key_columns)

    r = rdb_test.find_by_template(template, fields)

    print("*******************************")
    print("Result table:")
    pd.set_option('display.max_columns', 500);
    pd.set_option('display.width', 1000);
    print(pd.DataFrame(r));


def test_primary_key(test_name, database_name, table_name, key_columns, template, fields=None):
    print("\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)
    print("Fields = ", fields)
    print("*******************************")

    rdb_test = RDBDataTable_Pkg.RDBDataTable(database_name, table_name, key_columns)
    rdb_test.load()
    # the basic information of table
    print("Database name = ", rdb_test.database_name)
    print("Table name = ", rdb_test.table_name)
    print("Primary keys = ", rdb_test.key_columns)

    r = rdb_test.find_by_primary_key(template, fields)

    print("*******************************")
    print("Result table:")
    pd.set_option('display.max_columns', 500);
    pd.set_option('display.width', 1000);
    print(pd.DataFrame(r));


def test_insert(test_name, database_name, table_name, key_columns, row):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Row to insert = ", row)
    print("*******************************")

    rdb_test = RDBDataTable_Pkg.RDBDataTable(database_name, table_name, key_columns)
    rdb_test.load()
    # the basic information of table
    print("Database name = ", rdb_test.database_name)
    print("Table name = ", rdb_test.table_name)
    print("Primary keys = ", rdb_test.key_columns)
    rdb_test.insert(row)
    rdb_test.save()
    rdb_test.__str__();


def test_delete(test_name, database_name, table_name, key_columns, t):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Delete template = ", t)
    print("*******************************")

    rdb_test = RDBDataTable_Pkg.RDBDataTable(database_name, table_name, key_columns)
    rdb_test.load()
    # the basic information of table
    print("Database name = ", rdb_test.database_name)
    print("Table name = ", rdb_test.table_name)
    print("Primary keys = ", rdb_test.key_columns)
    rdb_test.delete(t)
    rdb_test.save()
    rdb_test.__str__();


if __name__ == "__main__":
    test_load_and_print("Load and print", "baseball", "PeopleSmall", ["playerID"]);
    test_insert("Insert", "baseball", "PeopleSmall", ["playerID"],{"playerID": "xt2233", "birthYear": "1996", "birthMonth": "5", "birthDay": "16", "birthCountry": "China",
                 "birthCity": "Nanjing","nameFirst":"Christine","nameLast":"Tan"});
    test_template("Find by Template - valid", "baseball", "PeopleSmall", ["playerID"], {"nameFirst":"David"});
    test_primary_key("Find by Primary Key - valid", "baseball", "PeopleSmall", ["playerID"], ["aardsda01"]);
    test_delete("Insert - valid", "baseball", "PeopleSmall", ["playerID"], {"nameFirst":"Christine"});


