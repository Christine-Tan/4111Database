
import CSVTableV2
import json
import sys, os

print (os.path.realpath('.'))

def test1():

    csvt = CSVTableV2.CSVTable("People", "PeopleSmall.csv", ["playerID"])
    csvt.load()
    print("Table = ", csvt)


def test_template(test_name, table_name, table_file, key_columns, template, fields=None, show_rows=False):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)
    print("Fields = ", fields)

    try:
        csvt = CSVTableV2.CSVTable(table_name, table_file, key_columns)
        csvt.load()

        if not show_rows:
            print("Table name = ", csvt.table_name)
            print("Table file = ", csvt.table_file)
            print("Table keys = ", csvt.key_columns)
        else:
            print(csvt)

        r = csvt.find_by_template(template, fields)
        print("Result table:")
        print(r)
    except ValueError as ve:
        print("Exception = ", ve)


def test_insert(test_name, table_name, table_file, key_columns, row, show_rows=False):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Row to insert = ", row)

    try:
        csvt = CSVTableV2.CSVTable(table_name, table_file, key_columns)
        csvt.load()

        if not show_rows:
            print("Table name = ", csvt.table_name)
            print("Table file = ", csvt.table_file)
            print("Table keys = ", csvt.key_columns)
        else:
            print(csvt)

        r = csvt.insert(row)
        print("Result table:")

        csvt.save()

        if show_rows:
            print(r)

    except ValueError as ve:
        print("Exception = ", ve)


#test1()


def test_templates():
    test_template("Test2", "People", "People.csv", ["playerID"],
                  {"birthMonth": "9", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"],
                  False)

    test_template("Test3", "People", "People.csv", ["playerID"],
                  {"nameFirst": "Ted", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"],
                  False)

    test_template("Test4", "People", "People.csv", ["canary"],
                  {"nameFirst": "Ted", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"],
                  False)

    test_template("Test5", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01"}, ["playerID", "yearID", "teamID", "AB", "H", "HR"],
                  False)

    test_template("Test6", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01", "iq": 100}, ["playerID", "yearID", "teamID", "AB", "H", "HR"],
                  False)

    test_template("Test7", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01", "yearID": "1961"}, ["playerID", "yearID", "teamID", "AB", "H", "HR"],
                  False)

    test_template("Test7", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01", "yearID": "1960"}, ["playerID", "yearID", "teamID", "AB", "H", "HR", "Age"],
                  False)


def test_inserts():

    test_insert("Insert Test 1", "People", "PeopleSmall.csv", ["playerID"],
                {"playerID": "dff1", "nameLast": "Ferguson", "nameFirst": "Donald"},
                False)

    test_template("Find after insert 1", "People", "PeopleSmall.csv", ["playerID"],
                  {"nameLast": "Ferguson"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"],
                  False)

    try:
        test_insert("Insert Test 2", "People", "PeopleSmall.csv", ["playerID"],
                    {"playerID": "dff1", "nameLast": "Ferguson", "nameFirst": "Donald"},
                    False)

        raise ValueError("That insert should not have worked!")

    except ValueError as ve:
        print("OK. Did not insert duplicate key.")


    test_insert("Insert Test 3", "Batting", "BattingSmall.csv", ["playerID", "yearID", "teamID", "stint"],
                {"playerID": "dff1", "teamID": "BOS", "yearID": "2018", "stint": "1",
                    "AB": "100", "H": "100"},
                False)

    test_template("Find after insert 3", "Batting", "BattingSmall.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "dff1"}, None,
                  False)


test_inserts()

