import json
import my_app.model as model


def test_find_by_template():
    result = model.find_by_template("people", dict({"playerID": ["xt2215"]}), None);
    print("Result = ", json.dumps(result))


def test_find_by_primary_key():
    result = model.find_by_primary_key("people", ['xt2215'], None);
    print("Result = ", json.dumps(result))


def test_insert():
    data = dict({"playerID": "xt2215", "nameFirst": "Xinyue", "nameLast": "Tan"})
    result = model.insert_resource("people", data)
    print("Result = ", result)


def test_update():
    data = dict({"nameFirst": "Xinyue"})
    result = model.update_by_primary_key("people", ["xt2215"], data);
    print("Result = ", result)


def test_delete():
    result = model.delete_by_primary_key("people", ["xt2215"]);
    print("Result = ", result)


def test_keys():
    result = model.get_key_columns("lahman", "Batting");
    print(result);


def test_find_by_related_resource():
    result = model.find_related_resources('batting', ["willite01", "1960", "BOS", "1"], 'people', {},
                                          "nameLast,  nameFirst,  birthYear,  throws", 0, 10)
    print(len(result))
    print(result);


def test_insert_related_resource():
    result = model.insert_related_resource('people', ["xt2215"], 'batting',
                                           {"teamID": "ALT", "yearID": "1884", "stint": "1"});
    print(result);


def test_find_teammates():
    result = model.find_teammates('willite01');
    print(json.dumps(result,indent=2))


def test_find_career_stats():
    result = model.find_career_stats('willite01');
    print(json.dumps(result,indent=2))


def test_find_roster():
    result = model.find_roster('BOS','2004');
    print(json.dumps(result,indent=2));

# test_find_by_primary_key()
# test_update()
# test_insert()
# test_delete()
# test_keys()
# test_find_by_related_resource()
# test_insert_related_resource()
# test_find_teammates()
# test_find_career_stats()
# test_find_roster()