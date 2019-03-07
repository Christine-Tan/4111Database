# view.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy
import my_app.model as model

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

DEFAULT_LIMIT = 10


# custom1
@app.route('/api/teammates/<player_id>', methods=["GET"])
def get_teammate(player_id):
    """
    show IDs, names, first year, last year and count of seasons as teammates
    for every player that was a teammate of player_id on any team in any season.
    :param player_id:
    :return:
    """
    args_dict = dict(request.args);

    current_url = request.url;
    args_dict, offset, limit = get_page_info(args_dict);
    previous_url = get_previous_link(args_dict, offset, limit);
    next_url = get_next_link(args_dict, offset, limit);
    result = model.find_teammates(player_id);

    template = render_template(result, previous_url, current_url, next_url, offset, limit);

    if result is not None:
        return json.dumps(template,indent=2), 200, {'Content-Type': 'application/json; charset=utf-8'}
    return "Not found teammates of" + player_id, 418, {'Content-Type': 'application/json; charset=utf-8'}


# custom2
@app.route('/api/people/<player_id>/career_stats', methods=["GET"])
def get_career_stats(player_id):
    """
    a summary of the career stats for a player.
    :param player_id:
    :return:
    """
    args_dict = dict(request.args);

    current_url = request.url;
    args_dict, offset, limit = get_page_info(args_dict);
    previous_url = get_previous_link(args_dict, offset, limit);
    next_url = get_next_link(args_dict, offset, limit);
    result = model.find_career_stats(player_id);
    template = render_template(result, previous_url, current_url, next_url, offset, limit);

    if result is not None:
        return json.dumps(template,indent=2), 200, {'Content-Type': 'application/json; charset=utf-8'}
    return "Not found career stats of" + player_id, 418, {'Content-Type': 'application/json; charset=utf-8'}


# base resource & custom3 roster
@app.route('/api/<resource>', methods=["GET", "POST"])
def get_resource(resource):
    # custom3 /api/roster?teamid=<team_id>&yearid=<year_id>
    if resource == "roster":
        # returns the roster and stats for a team in a year.
        args_dict = dict(request.args);
        team_id = args_dict['teamid'][0];
        year_id = args_dict['yearid'][0];
        current_url = request.url;
        args_dict, offset, limit = get_page_info(args_dict);
        previous_url = get_previous_link(args_dict, offset, limit);
        next_url = get_next_link(args_dict, offset, limit);

        result = model.find_roster(team_id, year_id);
        template = render_template(result, previous_url, current_url, next_url, offset, limit);
        if result is not None:
            return json.dumps(template,indent=2), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "Not found roster of" + json.dumps(args_dict), 418, {
                'Content-Type': 'application/json; charset=utf-8'}

    elif request.method == "GET":
        # find sth
        args_dict = dict(request.args);

        current_url = request.url;
        args_dict, offset, limit = get_page_info(args_dict);
        previous_url = get_previous_link(args_dict, offset, limit);
        next_url = get_next_link(args_dict, offset, limit);
        # fields
        args_dict, fields = get_fields(args_dict)
        # sql data
        result = model.find_by_template(resource, args_dict, fields, offset, limit + 1);

        template = render_template(result, previous_url, current_url, next_url, offset, limit);
        if result is not None:
            return json.dumps(template,indent=2), 200, {'Content-Type': 'application/json; charset=utf-8'}

    elif request.method == "POST":
        # insert sth
        data = request.get_data();
        if data is not None:
            args = dict(json.loads(data));
            model.insert_resource(resource, args);
            return "Successfully post with" + json.dumps(args), 200, {'Content-Type': 'application/json; charset=utf-8'}

    return "Not found in" + resource, 418, {'Content-Type': 'application/json; charset=utf-8'}


# Specific resource
@app.route('/api/<resource>/<primary_keys>', methods=["GET", "PUT", "DELETE"])
def get_by_primary_key(resource, primary_keys):
    if request.method == "GET":
        values = primary_keys.split("_");
        fields = request.args.get('fields', None);
        result = model.find_by_primary_key(resource, values, fields,0,1);
        if result is not None:
            return json.dumps(result,indent=2), 200, {'Content-Type': 'application/json; charset=utf-8'}

    elif request.method == "PUT":
        values = primary_keys.split("_");
        data = request.get_data();
        if data is not None:
            args = dict(json.loads(data));
            result = model.update_by_primary_key(resource, values, args);
            if result is not None:
                return "Successfully put with" + json.dumps(args), 200, {
                    'Content-Type': 'application/json; charset=utf-8'}

    elif request.method == "DELETE":
        values = primary_keys.split("_");
        result = model.delete_by_primary_key(resource, values);
        if result is not None:
            return "Successfully delete", 200, {
                'Content-Type': 'application/json; charset=utf-8'};

    return "Not found in" + resource, 418, {'Content-Type': 'application/json; charset=utf-8'}


# Related resources
@app.route('/api/<resource>/<primary_keys>/<relate>', methods=["GET", "POST"])
def get_related_resource(resource, primary_keys, relate):
    if request.method == "GET":
        # find sth
        args_dict = dict(request.args);

        current_url = request.url;
        args_dict, offset, limit = get_page_info(args_dict);
        previous_url = get_previous_link(args_dict, offset, limit);
        next_url = get_next_link(args_dict, offset, limit);
        args_dict, fields = get_fields(args_dict);

        values = primary_keys.split('_');
        results = model.find_related_resources(resource, values, relate, args_dict, fields, offset, limit);
        template = render_template(results, previous_url, current_url, next_url, offset, limit);
        return json.dumps(template,indent=2), 200, {'Content-Type': 'application/json; charset=utf-8'}
    elif request.method == "POST":
        # insert sth
        data = request.get_data();
        if data is not None:
            args = dict(json.loads(data));
            values = primary_keys.split('_');
            result = model.insert_related_resource(resource, values, relate, args);
            if result is not None:
                return "Successfully post with" + json.dumps(args), 200, {
                    'Content-Type': 'application/json; charset=utf-8'}

    return "Not found in" + resource, 418, {'Content-Type': 'application/json; charset=utf-8'}


def get_fields(args_dict):
    # fields
    fields = args_dict.get('fields', None);
    if fields is not None:
        del (args_dict["fields"]);
        fields = fields[0];
    return args_dict, fields


def get_page_info(args_dict):
    # pagination
    offset = args_dict.get('offset', None);
    limit = args_dict.get('limit', None);
    if offset is not None:
        del (args_dict['offset']);
        offset = int(offset[0]);
    else:
        offset = 0;

    if limit is not None:
        del (args_dict['limit']);
        limit = int(limit[0])
        if limit > DEFAULT_LIMIT:
            limit = DEFAULT_LIMIT;
    else:
        limit = DEFAULT_LIMIT;
    return args_dict, offset, limit;


def get_previous_link(args_dict, offset, limit):
    # previous link
    previous_args = "";
    for arg in args_dict.keys():
        expr = str(arg) + "=" + str(args_dict[arg][0]);
        previous_args += expr + '&';
    previous_args += "offset=" + str(offset - limit);
    previous_args += "&limit=" + str(limit);
    previous_url = request.path + "?" + previous_args;
    return previous_url;


def get_next_link(args_dict, offset, limit):
    # next link
    next_args = "";
    for arg in args_dict.keys():
        expr = str(arg) + "=" + str(args_dict[arg][0]);
        next_args += expr + '&';
    next_args += "offset=" + str(offset + limit);
    next_args += "&limit=" + str(limit);
    next_url = request.path + "?" + next_args;
    return next_url;


def render_template(result, previous_url, current_url, next_url, offset, limit):
    template = dict();
    template['current'] = current_url;

    if offset >= limit:
        template['previous'] = previous_url;

    if len(result) > limit:
        template['data'] = json.dumps(result[0:limit]);
        template['next'] = next_url;
    else:
        template['data'] = json.dumps(result);
    return template;


if __name__ == '__main__':
    app.run()
