# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import request, jsonify
import os
from . import models
from . import reviewers


def authors():
    if request.method == 'GET':
        persons = request.args.getlist('person')
        return jsonify(models.Authors.get(persons))
    elif request.method == 'POST':
        token = request.headers.get('token', '')
        if token == os.environ.get('POST_TOKEN', ''):
            return jsonify(models.Authors.post(request.get_json()))
        else:
            return jsonify(models.Authors.get(request.get_json()))
    return jsonify({})


def filestats():
    if request.method == 'GET':
        files = request.args.getlist('file')
        return jsonify(models.FilesStats.get(files))
    elif request.method == 'POST':
        token = request.headers.get('token', '')
        if token == os.environ.get('POST_TOKEN', ''):
            return jsonify(models.FilesStats.post(request.get_json()))
        else:
            return jsonify(models.FileStats.get(request.get_json()))
    return jsonify({})


def reviewer():
    if request.method == 'POST':
        return jsonify(reviewers.get(request.get_json()))
    return jsonify({})


def top():
    if request.method == 'GET':
        files = request.args.getlist('file')
        number = int(request.args.get('number', 5))
        return jsonify(reviewers.top(files, number))
    elif request.method == 'POST':
        return jsonify(reviewers.top(request.get_json()))
    return jsonify({})
