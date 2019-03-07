from redis_cache import data_cache
from utils import utils as ut

ut.set_debug_mode(False)

t = {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}
r = data_cache.compute_key("people", {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}, \
                           ['nameLast', "birthCity"])

template = {
    "nameLast": "Williams",
    "nameFirst": "Ted"
}

fields = ['playerID', 'nameFirst', 'bats', 'birthCity']

query_result = {'playerID': 'willite01', 'nameFirst': 'Ted', 'bats': 'L', 'birthCity': 'San Diego'}


def test1():
    data_cache.add_to_cache(r, t)


def test2():
    result = data_cache.get_from_cache(r)
    print("Result = ", result)


def test3():
    data_cache.add_to_query_cache("people", template, fields,query_result)
    result = data_cache.check_query_cache("people", template,fields)
    print("Result = ", result)

test1()
test2()
test3()