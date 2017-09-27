# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import six
import sqlalchemy.dialects.postgresql as pg
from .app import db, app


class Authors(db.Model):
    __tablename__ = 'authors'

    # hgname, bzname
    hgname = db.Column(db.String(512), primary_key=True)
    bzname = db.Column(db.String(256))

    def __init__(self, hgname, bzname):
        self.hgname = hgname
        self.bzname = bzname

    def __repr__(self):
        s = '<Author hg: {}, bz: {}>'
        return s.format(self.hgname,
                        self.bzname)

    @staticmethod
    def post(data):
        # data is a dict: {'command': 'update' or 'create',
        #                  'data': {'toinsert': hgname => bzname,
        #                           'torm': [...]}}
        cmd = data['command']
        toinsert = data['data']['toinsert']
        if toinsert:
            for hgname, bzname in toinsert.items():
                if cmd == 'create':
                    db.session.add(Authors(hgname, bzname))
                else:
                    ins = pg.insert(Authors).values(hgname=hgname,
                                                    bzname=bzname)
                    upd = ins.on_conflict_do_update(index_elements=['hgname'],
                                                    set_=dict(bzname=bzname))
                    db.session.execute(upd)
            db.session.commit()

        torm = data['data']['torm']
        if torm:
            query = db.session.query(Authors)
            persons = query.filter(Authors.hgname.in_(torm))
            persons.delete(synchronize_session=False)
            db.session.expire_all()
            db.session.commit()

        return {'error': ''}

    @staticmethod
    def get(hgnames=[]):
        if not hgnames:
            persons = db.session.query(Authors).all()
            res = {p.hgname: p.bzname for p in persons}
            return {'bznames': res,
                    'error': ''}

        if isinstance(hgnames, dict):
            if 'persons' in hgnames:
                hgnames = hgnames['persons']
            else:
                return {'bznames': {},
                        'error': 'A dictionary with key \'persons\' expected'}

        # hgname is a list of string or a single string
        if not isinstance(hgnames, list):
            hgnames = [hgnames]

        for name in hgnames:
            if not isinstance(name, six.string_types):
                return {'bznames': {},
                        'error': 'Strings expected'}

        persons = db.session.query(Authors)
        persons = persons.filter(Authors.hgname.in_(hgnames)).all()
        res = {p.hgname: p.bzname for p in persons}
        return {'bznames': res,
                'error': ''}


class FilesStats(db.Model):
    __tablename__ = 'filesstats'

    filename = db.Column(db.String(512), primary_key=True)
    author = db.Column(db.String(256), primary_key=True)
    score = db.Column(db.Float)

    def __init__(self, filename, author, score):
        self.filename = filename
        self.author = author
        self.score = score

    def __repr__(self):
        s = '<FileStat filename: {}, author: {}, score: {}>'
        return s.format(self.filename,
                        self.author,
                        self.score)

    @staticmethod
    def post(data):
        # data is a dict: {'command': 'update' or 'create',
        #                  'data': filename => {author => score}}
        cmd = data['command']
        data = data['data']
        for filename, scores in data.items():
            for person, score in scores.items():
                if cmd == 'create':
                    db.session.add(FilesStats(filename, person, score))
                else:
                    ins = pg.insert(FilesStats).values(filename=filename,
                                                       author=person,
                                                       score=score)
                    upd = ins.on_conflict_do_update(index_elements=['filename',
                                                                    'author'],
                                                    set_=dict(score=score))
                    db.session.execute(upd)
        db.session.commit()
        return {'error': ''}

    @staticmethod
    def get(filenames):
        if not filenames:
            return {'stats': {},
                    'error': 'No filenames specified'}

        if isinstance(filenames, dict):
            if 'filenames' in filenames:
                filenames = filenames['filenames']
            else:
                error = 'A dictionary with key \'filenames\' expected'
                return {'stats': {},
                        'error': error}

        # hgname is a list of string or a single string
        if not isinstance(filenames, list):
            filenames = [filenames]

        for name in filenames:
            if not isinstance(name, six.string_types):
                return {'stats': {},
                        'error': 'Strings expected'}

        files = db.session.query(FilesStats)
        files = files.filter(FilesStats.filename.in_(filenames)).all()
        res = {}
        for f in files:
            name = f.filename
            if name not in res:
                res[name] = {}
            res[name][f.author] = f.score

        return {'stats': res,
                'error': ''}


def create():
    e = db.get_engine(app)
    d = e.dialect
    if not d.has_table(e, 'authors') or not d.has_table(e, 'filestats'):
        db.create_all()
