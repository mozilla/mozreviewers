# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import json
from libmozdata import gmail, utils as lmdutils
import logging
import os
import requests

from .hgdata import get_hg_info
from .bzdata import get_bugs_info
from .authors import get_map_hg_bz


def push(payload, service, post_info):
    url = post_info['url']
    if not url.endswith('/'):
        url += '/'
    url += service

    token = post_info['token']
    headers = {'token': token,
               'content-type': 'application/json'}
    r = requests.post(url, data=json.dumps(payload), headers=headers)
    if r.status_code != requests.codes.ok:
        msg = 'Cannot post the data on {}, status_code is {}'
        msg = msg.format(url, r.status_code)
        raise Exception(msg)


def push_diff_authors(diff, post_info):
    payload = {'command': 'update',
               'data': diff}
    return push(payload, 'authors', post_info)


def push_diff_files(diff, post_info):
    payload = {'command': 'update',
               'data': diff}
    return push(payload, 'filestats', post_info)


def update_file_stats(patches, buginfo, mapping,
                      fstats_path, post_info, jsons):
    logging.info('Update file stats')
    with open(fstats_path, 'r') as In:
        old = json.load(In)

    diff_files = defaultdict(lambda: set())
    for patch in patches:
        bugid = patch['bugid']
        if bugid not in buginfo:
            continue

        author = patch['author']
        files = patch['files']
        touched = files['touched']
        added = files['added']
        moved = files['moved']

        reviewers = buginfo[bugid]['reviewers']
        bzauthor = mapping[author]
        files = touched

        for f in touched:
            if f not in old:
                old[f] = {}

        for f in added:
            files.append(f)
            old[f] = {}

        for o, n in moved.items():
            files.append(n)
            old[n] = old[o] if o in old else {}
            diff_files[n] |= set(old[n].keys())

        for f in files:
            for reviewer in reviewers:
                diff_files[f].add(reviewer)
                if reviewer in old[f]:
                    old[f][reviewer] += 0.4
                else:
                    old[f][reviewer] = 0.4

            diff_files[f].add(bzauthor)
            if bzauthor in old[f]:
                old[f][bzauthor] += 0.6
            else:
                old[f][bzauthor] = 0.6

    diff = defaultdict(lambda: dict())
    for f, authors in diff_files.items():
        for author in authors:
            diff[f][author] = old[f][author]
    diff = dict(diff)

    if diff:
        push_diff_files(diff, post_info)

    jsons[fstats_path] = old


def update_mapping(stats, mapping_path, post_info, jsons):
    logging.info('Update mapping')
    full_mapping = get_map_hg_bz(stats)
    mapping = remove_obsolete(full_mapping, stats['stats'])
    old = {}

    if os.path.isfile(mapping_path):
        with open(mapping_path, 'r') as In:
            old = json.load(In)

    torm = set(old.keys()) - set(mapping.keys())
    diff = {'torm': list(torm),
            'toinsert': {}}
    for hga, bza in mapping.items():
        if hga not in old or old[hga] != bza:
            diff['toinsert'][hga] = bza

    logging.info('Diff mapping: {}'.format(diff))
    if diff['torm'] or diff['toinsert']:
        push_diff_authors(diff, post_info)

    jsons[mapping_path] = mapping

    return full_mapping


def update_last_date(stats, patches):
    stats = stats['stats']
    for patch in patches:
        hgauthor = patch['author']
        date = patch['date']
        date = lmdutils.get_date_ymd(date)
        last = stats[hgauthor]['last_patch_date']
        if not last:
            stats[hgauthor]['last_patch_date'] = patch['date']
        else:
            last = lmdutils.get_date_ymd(last)
            if date > last:
                stats[hgauthor]['last_patch_date'] = patch['date']


def remove_obsolete(mapping, stats):
    mapping = mapping.copy()
    rev = defaultdict(lambda: set())
    for k, v in mapping.items():
        rev[v].add(k)

    potential = set()
    for hgauthor, info in stats.items():
        last = info['last_patch_date']
        last = lmdutils.get_date_ymd(last)
        today = lmdutils.get_date_ymd('today')
        if (today - last).days > 92 and hgauthor in mapping:
            potential.add(hgauthor)

    for hgauthor in potential:
        bzauthor = mapping[hgauthor]
        if rev[bzauthor] <= potential:
            del mapping[hgauthor]

    return mapping


def get_stats(hgpath, data_path, jsons, useless=set()):
    if os.path.isfile(data_path):
        with open(data_path, 'r') as In:
            old = json.load(In)
            last_rev = old['last_rev']
    else:
        old = {'mailnames': {},
               'stats': {},
               'last_rev': ''}
        last_rev = '0'

    logging.info('Last revision: {}'.format(last_rev))
    last_rev, hgdata, bugids, patches = get_hg_info(hgpath,
                                                    last_rev,
                                                    rev='tip')
    logging.info('New last revision: {}'.format(last_rev))

    if last_rev:
        fields = ['attachers', 'commenters', 'reviewees']
        logging.info('Retrieve bugs information')
        bi = get_bugs_info(bugids)
        buginfo = bi['info']
        mailnames = bi['mailnames']
        old['last_rev'] = last_rev
        stats = old['stats']

        logging.info('Compute statistics')
        for hgauthor, bugids in hgdata.items():
            if hgauthor in useless or len(hgauthor) <= 3:
                continue

            if hgauthor not in stats:
                stats[hgauthor] = {'assignees': {},
                                   'attachers': {},
                                   'commenters': {},
                                   'reviewees': {},
                                   'last_patch_date': ''}
            stats_author = stats[hgauthor]
            for bugid in bugids:
                if bugid not in buginfo:
                    continue
                info = buginfo[bugid]
                assignee = info['assignee']
                if assignee:
                    if assignee not in stats_author['assignees']:
                        stats_author['assignees'][assignee] = 1
                    else:
                        stats_author['assignees'][assignee] += 1
                for f in fields:
                    for x, n in info[f].items():
                        if x not in stats_author[f]:
                            stats_author[f][x] = n
                        else:
                            stats_author[f][x] += n

        mailnames = old['mailnames']
        for bzmail, realnames in bi['mailnames'].items():
            if bzmail in mailnames:
                s = set(mailnames[bzmail]) | set(realnames)
                mailnames[bzmail] = list(s)
            else:
                mailnames[bzmail] = list(realnames)

        update_last_date(old, patches)
        jsons[data_path] = old

        return True, old, bi['info'], patches
    return False, old, None, None


def get_config(path='./config.json'):
    with open(path, 'r') as In:
        conf = json.load(In)
        conf['useless_authors'] = set(conf['useless_authors'])
    return conf


def update():
    conf = get_config()
    paths = conf['paths']
    jsons = {}
    logging.basicConfig(filename=paths['log'],
                        filemode='w',
                        level=logging.DEBUG,
                        format='%(asctime)s -- %(levelname)s -- %(message)s')

    try:
        useless = conf['useless_authors']
        changed, stats, buginfo, patches = get_stats(paths['hg'],
                                                     paths['authors_data'],
                                                     jsons,
                                                     useless=useless)
        if changed:
            mapping = update_mapping(stats, paths['mapping'],
                                     conf['post'], jsons)
            update_file_stats(patches, buginfo, mapping,
                              paths['files_stats'], conf['post'], jsons)
            for path, data in jsons.items():
                with open(path, 'w') as Out:
                    json.dump(data, Out)
    except:
        logging.error('An exception raised:', exc_info=True)
        date = lmdutils.get_today()
        title = 'Error in getting data for Mozilla reviewers the {}'
        title = title.format(date)
        body = 'The data for reviewers have not been updated due to an error.'
        gmail.send(conf['emails'], title, body, files=[paths['log']])
    finally:
        logging.shutdown()
        logging.getLogger(None).handlers = []
        os.remove(paths['log'])


if __name__ == '__main__':
    update()
