import pymysql

user = 'dbuser'
password = 'dbuser'
database = 'lahman2017raw'
# database = 'lahman'
# user = 'root'
# password = '123456'

cnx = pymysql.connect(host='localhost',
                      user=user,
                      password=password,
                      db=database,
                      charset='utf8mb4',
                      cursorclass=pymysql.cursors.DictCursor)


# people_primary_key = ["playerID"]
# teams_primary_key = ["teamID", "yearID"]
# appearances_primary_key = ["yearID", "playerID", "teamID"]
# batting_primary_key = ["playerID", "yearID", "teamID", "stint"]
# fielding_primary_key = ["playerID", "yearID", "teamID", "POS", "stint"]


def find_by_primary_key(resource, values, fields, offset, limit):
    key_columns = get_key_columns(resource);
    if key_columns is not None and len(key_columns) > 0:
        q = get_find_by_primary_key_sql(resource, key_columns, values, fields, offset, limit);
    else:
        return None;
    result = run_q(q, True);
    return result;


def find_by_template(resource, t, fields, offset, limit):
    query = get_find_by_template_sql(resource, t, fields, offset, limit);
    result = run_q(query, True);
    return result;


def insert_resource(resource, data):
    q = get_insert_sql(resource, data);
    run_q(q, False);
    return q


def update_by_primary_key(resource, values, data):
    key_columns = get_key_columns(resource);
    if (key_columns is not None) and len(key_columns) > 0:
        q = get_update_sql(resource, key_columns, values, data);
        run_q(q, False);
        return q
    return None


def delete_by_primary_key(resource, values):
    key_columns = get_key_columns(resource);
    if (key_columns is not None) and len(key_columns) > 0:
        q = get_delete_sql(resource, key_columns, values);
        run_q(q, False);
        return q;
    return None;


def find_related_resources(resource, values, related_resource, t, fields, offset, limit):
    resource_fields = get_reference_columns(resource, related_resource);
    if len(resource_fields) <= 0:
        resource_fields = get_reference_columns(related_resource, resource);
    if len(resource_fields) > 0:
        resource_results = find_by_primary_key(resource, values, resource_fields, 0, 1);
        if len(resource_results) > 0:
            resource_result = resource_results[0];
            for primary_key in resource_result.keys():
                value = list();
                value.append(resource_result[primary_key]);
                t[primary_key] = value;
            results = find_by_template(related_resource, t, fields, offset, limit);
            return results;
    return None;


def insert_related_resource(resource, values, related_resource, data):
    resource_fields = get_reference_columns(resource, related_resource);
    if len(resource_fields) <= 0:
        resource_fields = get_reference_columns(related_resource, resource);
    if len(resource_fields) > 0:
        resource_results = find_by_primary_key(resource, values, resource_fields, 0, 1);
        if len(resource_results) > 0:
            resource_result = resource_results[0];
            for primary_key in resource_result.keys():
                data[primary_key] = resource_result[primary_key];
            results = insert_resource(related_resource, data);
            return results;
    return None;


def find_teammates(player_id):
    q = "SELECT player.playerID,player.nameFirst,player.nameLast," \
        "a2.playerID as teammate_id,p.nameFirst as teammate_nameFirst,p.nameLast as teammate_nameLast," \
        "min(a2.yearID) firstYear,max(a2.yearID) lastYear, count(a2.yearID)" + \
        " FROM (SELECT a1.playerID,p1.nameFirst, p1.nameLast, a1.yearID,a1.teamID" + \
        " FROM Appearances a1,People p1" + \
        " WHERE a1.playerID =\'" + player_id + "\' and p1.playerID=a1.playerID) player, Appearances a2,People p" + \
        " WHERE player.yearID=a2.yearID AND player.teamID=a2.teamID AND player.playerID<>a2.playerID " \
        "AND p.playerID = a2.playerID" + \
        " GROUP BY a2.playerID;"
    result = run_q(q, True)
    return result;


def find_career_stats(player_id):
    q = "SELECT a.playerID,a.yearID,a.teamID,a.G_all,b.H as Hits,b.AB,f.A as assists,f.E as errors" + \
        " FROM (SELECT playerID, yearID,teamID,G_all" + \
        " FROM Appearances" + \
        " WHERE playerID = \'" + player_id + "\') a, Batting b, Fielding f" + \
        " WHERE a.playerID=b.playerID AND a.yearID=b.yearID AND a.teamID=b.teamID" + \
        " AND a.playerID=f.playerID AND a.yearID=f.yearID AND a.teamID=f.teamID;"
    result = run_q(q, True);
    return result;


def find_roster(team_id, year_id):
    q = "SELECT a_all.nameFirst, a_all.nameLast, a_all.playerID, a_all.teamID, a_all.yearID, a_all.G_all, " + \
        " a_all.hits, a_all.AB, f_all.assists, f_all.errors" + \
        " FROM (SELECT p.nameFirst, p.nameLast , a.playerID, a.teamID, a.yearID,  a.G_all, b.H AS hits, b.AB" + \
        " FROM Appearances a, Batting b, People p" + \
        " WHERE a.teamID = \'" + team_id + "\' AND a.yearID = \'" + year_id + \
        "\' AND b.yearID = a.yearID AND b.teamID = a.teamID AND" + \
        " b.playerID = a.playerID AND p.playerID = a.playerID) AS a_all" + \
        " JOIN (SELECT f.playerID, f.teamID, f.yearID, sum(f.A) AS assists, sum(f.E) AS errors" + \
        " FROM Fielding f" + \
        " WHERE f.yearID = \'" + year_id + "\' AND f.teamID = \'" + team_id + "\'" + \
        " GROUP BY f.playerID) AS f_all" + \
        " ON f_all.playerID = a_all.playerID AND f_all.yearID = a_all.yearID AND f_all.teamID = a_all.teamID;"
    result = run_q(q, True)
    return result;


def get_find_by_primary_key_sql(resource, key_columns, values, fields, offset, limit):
    t = value2template(key_columns, values);
    return get_find_by_template_sql(resource, t, fields, offset, limit);


def get_find_by_template_sql(table_name, t, fields, offset, limit):
    """
    Return a sql statement of
    find by template
    """
    where = template2where(t);
    if fields is None:
        query_sql = "SELECT * FROM " + table_name + " " + where + " LIMIT " + \
                    str(limit) + " OFFSET " + str(offset) + ";";
    else:
        query_sql = "SELECT " + fields + " FROM " + table_name + " " + where + " LIMIT " + str(
            limit) + " OFFSET " + str(offset) + ";";
    return query_sql;


def get_insert_sql(table_name, data):
    """
    return the sql statement of
    insert into <table_name> args
    """
    fields = list2str(data.keys());
    values = list();
    for key in data.keys():
        values.append("\"" + str(data[key]) + "\"");
    insert_sql = "INSERT INTO " + table_name + " (" + fields + ")" + " VALUES " + "(" + list2str(
        values) + ");"
    return insert_sql;


def get_update_sql(table_name, key_columns, values, data):
    t = value2template(key_columns, values);
    where_expr = template2where(t);
    set_expr = dict2str(data);
    update_sql = "UPDATE " + table_name + " SET " + set_expr + " " + where_expr + ";";
    return update_sql;


def get_delete_sql(table_name, key_columns, values):
    t = value2template(key_columns, values);
    where = template2where(t);
    delete_sql = "DELETE FROM " + table_name + " " + where + ";";
    return delete_sql;


def get_related_values(resource, values, related_resource, related_values):
    resource_keys = get_key_columns(resource);
    t = value2template(resource_keys, values);

    related_keys = get_key_columns(related_resource);
    new_related_values = list();
    index = 0;
    for related_key in related_keys:
        if related_key in t.keys():
            new_related_values.append(t[related_key][0]);
        else:
            new_related_values.append(related_values[index]);
            index += 1;
    return related_keys, new_related_values;


def list2str(l):
    result = "";
    for s in l:
        result += str(s) + ",";
    return result[0:len(result) - 1];


def dict2str(d):
    result = "";
    for key in d.keys():
        expr = str(key) + "=" + "\"" + str(d.get(key)) + "\"";
        result += expr + ",";
    return result[0:len(result) - 1];


def value2template(key_columns, values):
    if len(values) != len(key_columns):
        raise ValueError(values, "incompatible values");
    i = 0;
    t = dict();
    for key in key_columns:
        value = list();
        value.append(values[i]);
        t[key] = value;
        i += 1;
    return t;


def template2where(args):
    s = ""
    for (k, v) in args.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"
    if s != "":
        s = "WHERE " + s;
    return s;


def get_key_columns(table_name):
    q = "SELECT k.column_name" + \
        " FROM information_schema.table_constraints t" + \
        " JOIN information_schema.key_column_usage k" + \
        " USING (constraint_name,table_schema,table_name)" + \
        " WHERE t.constraint_type='PRIMARY KEY'" + \
        " AND t.table_schema = (SELECT database())" + \
        " AND t.table_name =\'" + table_name + "\';";
    results = run_q(q, True);
    key_columns = list();
    for result in results:
        key_columns.append(result['column_name']);
    return key_columns;


def get_reference_columns(table_name, related_table_name):
    q = "SELECT k.referenced_column_name" + \
        " FROM information_schema.table_constraints t" + \
        " JOIN information_schema.key_column_usage k" + \
        " USING (constraint_name,table_schema,table_name)" + \
        " WHERE t.constraint_type='FOREIGN KEY'" + \
        " AND t.table_schema=(select database())" + \
        " AND t.table_name = \'" + table_name + \
        "\' AND k.referenced_table_name = \'" + related_table_name + "\';"

    results = run_q(q, True);

    reference_columns = "";
    if results is not None and len(results) > 0:
        for result in results:
            reference_columns += str(result['referenced_column_name']);
    return reference_columns;


def run_q(q, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result;
