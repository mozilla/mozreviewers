# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import hglib
import re

from .patch_analysis import get_files


BUG_PAT = re.compile(r'bug[\t ]*([0-9]+)', re.I)
BUG_WITH_R_1_PAT = re.compile(r'\(bug[\t #]*([0-9]+)[ \t,;\.]*'
                              r'(?:r|sr|a|p)=[^\)]+\)', re.I)
BUG_WITH_R_2_PAT = re.compile(r'\(bug[\t #]*([0-9]+)\)[ \t,;\.]*'
                              r'(?:r|sr|a|p)*[^=]*=', re.I)
BUG_WITH_R_3_PAT = re.compile(r'b=([0-9]+)[ \t,;\.]*'
                              r'(?:r|sr|a|p)[^=]*=', re.I)
BUG_WITH_R_4_PAT = re.compile(r'b=([0-9]+).?$', re.I)
FIX_PAT = re.compile(r'^(?:fixup for|fix for|fixup|fixing|fixes|fix)[ \t,;\.]*'
                     r'(?:bug)?[ \t,;\.#]*([0-9]+)', re.I)
MAIN_BUG_PAT = re.compile(r'^(?:(?:\[[^\]]*\][ \t]*)|.|..)?'
                          r'(?:bug bug|bugzilla bug|bugzilla|bugs|bug|b)'
                          r'[\t =\-\_:#\u00a0]*([0-9]+)', re.I | re.UNICODE)
BACKOUT_PAT = re.compile(r'(?:(?:back(?:ed|ing|s)?(?:[ _]*out[_]?))'
                         r'|(?:revert(?:ing|s)?)) '
                         r'(?:(?:cset|changeset|revision|rev|of)s?)?'
                         r'(.+)', re.I | re.DOTALL)


def get_bug_from(desc):
    m = BACKOUT_PAT.search(desc)
    if m:
        return 0

    main = 0
    m = MAIN_BUG_PAT.search(desc)
    if m:
        main = int(m.group(1))
    elif desc.startswith('servo: Merge #'):
        m = BUG_PAT.search(desc)
        if m:
            main = int(m.group(1))
    else:
        pats = [BUG_WITH_R_1_PAT, BUG_WITH_R_2_PAT,
                BUG_WITH_R_3_PAT, BUG_WITH_R_4_PAT,
                FIX_PAT]
        for pat in pats:
            m = pat.search(desc)
            if m:
                main = int(m.group(1))
                break

    return main


def get_hg_info(hgpath, last_rev, rev='tip'):
    client = hglib.open(hgpath)
    client.pull(update=True)
    revrange = '{}:{}'.format(rev, last_rev)
    out = client.log(revrange=revrange, nomerges=True)
    last_rev, res, bugids, patches = None, None, None, None

    # remove the last entry which corresponds to last_rev
    out = out[:-1]
    if out:
        last_rev = out[0][1]
        last_rev = last_rev.decode('ascii')
        res = defaultdict(lambda: set())
        patches = []
        bugids = set()
        for o in out[:-1]:
            # rev, node, tags, branch, author, desc, date
            rev, _, _, _, author, desc, date = o
            desc = desc.decode('utf-8')
            author = author.decode('utf-8')
            bugid = get_bug_from(desc)
            if bugid:
                res[author].add(bugid)
                bugids.add(bugid)
                patch = client.export([rev])
                patch = patch.decode('utf-8')
                patches.append({'author': author,
                                'date': date.strftime('%Y-%m-%d'),
                                'files': get_files(patch),
                                'bugid': bugid})
        patches = patches[::-1]

    client.close()
    return last_rev, res, bugids, patches
