from src import CSVCatalog as CSVCatalog
import json
import pymysql


def test_column_def_to_json():
    print("\n*********** Testing column def to JSON. *******************\n")
    col = CSVCatalog.ColumnDefinition('teamID', 'text', True)
    print(col.to_json());


def test_index_def_to_json():
    print("\n*********** Testing index def to JSON. *******************\n")

    cds = []
    cds.append(CSVCatalog.ColumnDefinition('teamID', 'text', True))
    cds.append(CSVCatalog.ColumnDefinition('yearID', 'text', True))
    ind = CSVCatalog.IndexDefinition('INDEX_teamID_yearID', cds, "PRIMARY");
    print(ind.to_json());


def test_table_def_to_json():
    print("\n*********** Testing table def to JSON. *******************\n")
    cds = list()
    cds.append(CSVCatalog.ColumnDefinition('teamID', 'text', True))
    cds.append(CSVCatalog.ColumnDefinition('yearID', 'text', True))
    cds.append(CSVCatalog.ColumnDefinition('AB', 'number'))

    tbl = CSVCatalog.TableDefinition(
        "batting",
        "../data/Batting.csv",
        column_definitions=cds)
    print(tbl.to_json())


def test_table_def_load():
    cnx = pymysql.connect(host='localhost',
                          user='dbuser',
                          password='dbuser',
                          db='my_catalog',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    result = CSVCatalog.TableDefinition.load_table_definition(cnx, "People");
    print(result.to_json())


def test_catalog_create():
    catalog = CSVCatalog.CSVCatalog('localhost', 3306, 'my_catalog', 'dbuser', 'dbuser', debug_mode=None)
    catalog.drop_table("People")

    player = CSVCatalog.ColumnDefinition('playerID', 'text', True);
    name_first = CSVCatalog.ColumnDefinition('nameFirst', 'text', True);
    birth_year = CSVCatalog.ColumnDefinition('birthYear', 'number');
    name_last = CSVCatalog.ColumnDefinition('nameLast', 'text', True)

    columns = list()
    columns.append(player)
    columns.append(name_first)
    columns.append(birth_year)

    print("\n*********** Testing catalog create/drop table. *******************\n")
    re = catalog.create_table("People", "../data/People.csv", columns, None)
    print(re.to_json())
    # catalog.drop_table("PeopleSmall")

    print("\n*********** Testing catalog table. *******************\n")
    table = catalog.get_table("People");
    print(table.to_json())

    print("\n*********** Testing catalog add column. *******************\n")
    table.add_column_definition(name_last)
    print(table.to_json())

    print("\n*********** Testing catalog drop column. *******************\n")
    table.drop_column_definition("birthYear")
    print(table.to_json())

    print("\n*********** Testing catalog add primary key. *******************\n")
    table.define_primary_key(['playerID']);
    print(table.to_json())

    print("\n*********** Testing catalog add index. *******************\n")
    table.define_index("People_nameFirst_nameLast_index", ["nameFirst","nameLast"], "INDEX");
    print(table.to_json())

    print("\n*********** Testing catalog drop index. *******************\n")
    table.drop_index("People_nameFirst_nameLast_index");
    print(table.to_json());


test_catalog_create()
test_table_def_load()

test_column_def_to_json()
test_index_def_to_json()
test_table_def_to_json()
