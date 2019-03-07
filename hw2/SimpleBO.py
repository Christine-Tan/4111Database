import pymysql
import json

# cnx = pymysql.connect(host='localhost',
#                               user='dbuser',
#                               password='dbuser',
#                               db='lahman2017raw',
#                               charset='utf8mb4',
#                               cursorclass=pymysql.cursors.DictCursor)
cnx = pymysql.connect(host='localhost',
                              user='root',
                              password='123456',
                              db='lahman',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)

def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result


def find_people_by_primary_key(primary_key):
    q = "select * from people where playerid = %s"
    result = run_q(q, (primary_key), True)
    return result





