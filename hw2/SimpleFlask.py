# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request

import SimpleBO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)


@app.route('/api/people/<primary_key>')
def get_resource(primary_key):

    result = SimpleBO.find_people_by_primary_key(primary_key)

    if result:
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
    else:
        return "NOT FOUND", 404


if __name__ == '__main__':
    app.run()




