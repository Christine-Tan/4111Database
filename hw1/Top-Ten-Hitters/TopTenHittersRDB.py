import mysql.connector
import pandas as pd
import datetime

# connect to mysql
cnx = mysql.connector.connect(user='root', password='123456',
                              host='127.0.0.1', port=3306,
                              database="baseball");
cursor = cnx.cursor();


# TopTenHittersRDB:
def top_ten_hitters_rdb():
    top_ten_hitters_sql = "SELECT Batting.playerID, " \
                          "(SELECT People.nameFirst FROM People WHERE People.playerID=Batting.playerID) as first_name, " \
                          "(SELECT People.nameLast FROM People WHERE People.playerID=Batting.playerID) as last_name, " \
                          "sum(Batting.h)/sum(Batting.ab) as career_average, " \
                          "sum(Batting.h) as career_hits, " \
                          "sum(Batting.ab) as career_at_bats," \
                          "min(Batting.yearID) as first_year, " \
                          "max(Batting.yearID) as last_year " \
                          "FROM Batting " \
                          "GROUP BY playerId " \
                          "HAVING career_at_bats > 200 AND last_year >= 1960 " \
                          "ORDER BY career_average DESC " \
                          "LIMIT 10;";
    cursor.execute(top_ten_hitters_sql);
    return cursor.fetchall();


if __name__ == "__main__":
    start_time = datetime.datetime.now();
    results = top_ten_hitters_rdb();
    column_names = ["playerID", "nameFirst", "nameLast", "career_average", "career_hits", "career_at_bats",
                    "first_year",
                    "last_year"];
    pd.set_option('display.max_columns', 500);
    pd.set_option('display.width', 1000);
    print(pd.DataFrame(results, columns=column_names));
    end_time = datetime.datetime.now();
    print("Total running time is: " + str((end_time - start_time).seconds) + "s");
