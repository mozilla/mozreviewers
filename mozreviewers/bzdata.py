# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from libmozdata.bugzilla import Bugzilla
from collections import defaultdict
import re


REVIEW_PAT = re.compile(r'review\?\(([^\)]*)\)')


def bug_handler(bug, data):
    bugid = bug['id']
    data[bugid]['bug'] = bug


def comment_handler(bug, bugid, data):
    data[int(bugid)]['comment'] = bug


def history_handler(history, data):
    bugid = int(history['id'])
    data[bugid]['history'] = history


def get_bugs(bugids):
    data = {bugid: {} for bugid in bugids}
    Bugzilla(bugids=bugids,
             bughandler=bug_handler,
             bugdata=data,
             commenthandler=comment_handler,
             commentdata=data,
             historyhandler=history_handler,
             historydata=data).get_data().wait()
    return data


def get_attachers(comments, attachers, commenters):
    for comment in comments:
        author = comment['author']
        commenters[author] += 1
        if comment['attachment_id'] is not None:
            attachers[author] += 1


def get_assignee(bug):
    assignee = bug['assigned_to']
    if assignee != 'nobody@mozilla.org':
        return assignee
    return ''


def get_mail_name(mailnames, bug):
    details = bug.get('cc_detail', [])
    details.append(bug['creator_detail'])
    if get_assignee(bug):
        details.append(bug['assigned_to_detail'])
    for detail in details:
        mailnames[detail['email']].add(detail['real_name'])


def get_reviewers(history, reviewees, reviewers):
    for h in history:
        for c in h['changes']:
            fn = c['field_name']
            if fn == 'flagtypes.name' and c.get('attachment_id', ''):
                added = c['added']
                m = REVIEW_PAT.search(added)
                if m:
                    reviewee = h['who']
                    reviewees[reviewee] += 1
                    reviewer = m.group(1)
                    reviewers.add(reviewer)


def get_pc(bug):
    product = bug['product']
    component = bug['component']
    return product, component


def get_bugs_info(bugids):
    data = get_bugs(bugids)
    res = {}
    mailnames = defaultdict(lambda: set())
    for bugid, info in data.items():
        if 'bug' not in info:
            print(bugid, info)
            continue
        bug = info['bug']
        get_mail_name(mailnames, bug)
        comments = info['comment']['comments']
        history = info['history']['history']
        assignee = get_assignee(bug)
        commenters = defaultdict(lambda: 0)
        attachers = defaultdict(lambda: 0)
        get_attachers(comments, attachers, commenters)
        reviewees = defaultdict(lambda: 0)
        reviewers = set()
        get_reviewers(history, reviewees, reviewers)
        reviewers = list(reviewers)
        product, component = get_pc(bug)
        res[bugid] = {'assignee': assignee,
                      'attachers': attachers,
                      'commenters': commenters,
                      'reviewees': reviewees,
                      'reviewers': reviewers,
                      'product': product,
                      'component': component}

    return {'mailnames': mailnames,
            'info': res}
