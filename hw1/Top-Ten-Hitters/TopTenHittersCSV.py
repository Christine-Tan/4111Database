import os
import csv
import pandas as pd
import CSVDataTable.CSVDataTable as CSVDataTablePkg
import datetime

PLAYER_ID = "playerID";
YEAR_ID = "yearID";
BAT = "AB";
HIT = "H";
NAME_FIRST = "nameFirst";
NAME_LAST = "nameLast";
TOTAL_BAT = "career_bats";
TOTAL_HIT = "career_hits";
BATTING_AVG = "career_average";


def top_ten_hitters_csv():
    # init the files
    batting_file = CSVDataTablePkg.CSVDataTable("Batting", "Batting.csv",
                                                ["playerID", "teamID", "yearID", "stint"]);
    batting_file.load();
    people_file = CSVDataTablePkg.CSVDataTable("People", "People.csv", ["playerID"]);
    people_file.load();

    # save the processed ids
    processed_player_ids = list();
    # save the record
    records = list();
    # check the unique playerID in Batting
    for row in batting_file.table_data:
        play_id = row[PLAYER_ID];
        if play_id not in processed_player_ids:
            processed_player_ids.append(play_id);
            result_batting = batting_file.find_by_template({PLAYER_ID: play_id}, [YEAR_ID, BAT, HIT]);
            # check yearID>=1960 for this player
            flag = False;
            if len(result_batting) > 0:
                total_bats = 0;
                total_hits = 0;
                for r in result_batting:
                    if int(r[YEAR_ID]) >= 1960:
                        flag = True;
                    # compute total AB and total H
                    total_bats += int(r[BAT]);
                    total_hits += int(r[HIT]);
            # if this is a valid playerID
            if flag:
                # compute the batting average
                if total_bats > 200:
                    batting_avg = total_hits / total_bats;
                    # look up for the name
                    result_name = people_file.find_by_primary_key([play_id], [PLAYER_ID, NAME_FIRST, NAME_LAST]);
                    first_name = result_name[0][NAME_FIRST];
                    last_name = result_name[0][NAME_LAST];
                    record = dict(zip([PLAYER_ID, NAME_FIRST, NAME_LAST, TOTAL_BAT, TOTAL_HIT, BATTING_AVG],
                                      [play_id, first_name, last_name, total_bats, total_hits, batting_avg]));
                    records.append(record);

    sorted_records = sorted(records, key=lambda k: k[BATTING_AVG], reverse=True);
    return sorted_records[0:10];


if __name__ == "__main__":
    start_time = datetime.datetime.now();
    result = top_ten_hitters_csv();
    pd.set_option('display.max_columns', 500);
    pd.set_option('display.width', 1000);
    print(pd.DataFrame(result, columns=[PLAYER_ID, NAME_FIRST, NAME_LAST, TOTAL_BAT, TOTAL_HIT, BATTING_AVG]));
    end_time = datetime.datetime.now();
    print("Total running time is: " + str((end_time - start_time).seconds) + "s");
