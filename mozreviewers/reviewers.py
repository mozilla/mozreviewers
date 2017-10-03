# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
from libmozdata.bugzilla import BugzillaUser
from libmozdata.connection import Connection
import math
import re
import six

from .patch_analysis import analyze_patch
from .models import FilesStats, Authors
from .logger import logger


NICK_PAT = re.compile(r'(:[\w]+)')


def get_nick(authors):
    bz = {}

    def user_handler(u):
        real = u['real_name']
        m = NICK_PAT.search(real)
        nick = m.group(1) if m else ''
        name = u['name']
        bz[name] = {'name': name,
                    'real_name': real,
                    'nick_name': nick}

    authors = list(authors)
    queries = []
    for chunk in Connection.chunks(authors, 20):
        query = BugzillaUser(user_names=chunk,
                             include_fields=['name', 'real_name'],
                             user_handler=user_handler)
        queries.append(query)

    for q in queries:
        q.wait()

    authors = [bz[a] for a in authors if a in bz]
    return authors


def percent(scores):
    total = float(sum(scores.values()))
    percentages = {}
    for author, score in scores.items():
        percentages[author] = float(score) / total
    return percentages


def gather(filestats, authors):
    gathered_stats = defaultdict(lambda: 0.)
    for stats in filestats.values():
        for author, score in stats.items():
            gathered_stats[author] += score

    gathered_stats = percent(gathered_stats)
    # we remove all the bz authors who aren't in Authors
    # (because they don't commit anything in the last 3 months)
    # for information: we take into account the old devs to compute the score
    # of the actual devs to avoid to have specialists who made almost nothing
    bzauthors = set(authors.values())
    torm = [a for a in gathered_stats.keys() if a not in bzauthors]
    for a in torm:
        del gathered_stats[a]
    return gathered_stats


def get_top(stats, number):
    stats = sorted(stats.items(), key=lambda p: p[1], reverse=True)
    stats = list(stats)
    if len(stats) > number:
        stats = stats[:number]

    scores = [r[1] for r in stats]
    persons = [r[0] for r in stats]
    return persons, scores


def top(files, number=5):
    logger.info('Get top authors')
    if isinstance(files, dict) and 'files' in files:
        if 'number' in files:
            number = int(files['number'])
        files = files['files']
    if isinstance(files, six.string_types):
        files = [files]
    if not isinstance(files, list):
        files = list(files)
    filestats = FilesStats.get(files)['stats']
    authors = Authors.get()['bznames']
    stats = gather(filestats, authors)
    persons, scores = get_top(stats, number)
    persons = get_nick(persons)
    for p, s in zip(persons, scores):
        p['score'] = math.floor(s * 1000.) / 10.

    return {'top': persons,
            'error': ''}


def get(patch, number=5):
    logger.info('Get reviewers for patch')
    if isinstance(patch, dict) and 'patch' in patch:
        ishg = False
        if 'bzauthor' in patch:
            patch_author = patch['bzauthor']
        elif 'hgauthor' in patch:
            patch_author = patch['hgauthor']
            ishg = True

        if 'annotations' in patch:
            check_annotation = patch['annotations']
            if isinstance(check_annotation, six.string_types):
                check_annotation = check_annotation.lower()
                check_annotation = check_annotation == 'true'
            else:
                check_annotation = bool(check_annotation)
        else:
            check_annotation = True
        patch = patch['patch']
    else:
        return {'reviewers': [],
                'error': 'Invalid payload'}

    patch_stats, changed = analyze_patch(patch, check_annotation)
    changed = list(changed)
    filestats = FilesStats.get(changed)['stats']
    deleted = percent(patch_stats['deleted'])
    alllines = percent(patch_stats['all'])
    authors = Authors.get()['bznames']

    if ishg:
        patch_author = authors.get(patch_author, '')

    deleted = {authors[k]: n for k, n in deleted.items() if k in authors}
    alllines = {authors[k]: n for k, n in alllines.items() if k in authors}

    gathered_stats = gather(filestats, authors)

    # we compute the total score
    stats = defaultdict(lambda: 0.)
    names = [deleted, alllines, gathered_stats]
    for name in names:
        for author, score in name.items():
            stats[author] += score

    if patch_author in stats:
        del stats[patch_author]

    reviewers, scores = get_top(stats, number)
    reviewers = get_nick(reviewers)
    for r, s in zip(reviewers, scores):
        r['score'] = math.floor(s * 1000) / 10.

    return {'reviewers': reviewers,
            'error': ''}
