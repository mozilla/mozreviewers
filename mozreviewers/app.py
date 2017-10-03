# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import Flask
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
import os


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/authors', methods=['GET', 'POST'])
@cross_origin()
def authors():
    from . import api
    return api.authors()


@app.route('/filestats', methods=['GET', 'POST'])
@cross_origin()
def filestats():
    from . import api
    return api.filestats()


@app.route('/reviewers', methods=['POST'])
@cross_origin()
def reviewer():
    from . import api
    return api.reviewer()


@app.route('/top', methods=['GET', 'POST'])
@cross_origin()
def top():
    from . import api
    return api.top()


if __name__ == '__main__':
    app.run()
