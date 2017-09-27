# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from copy import deepcopy
import logging
from multiprocessing import Pool, cpu_count
from nltk.util import ngrams
import re
from sklearn.feature_extraction.text import TfidfVectorizer


PAT = re.compile('<|>|@|\.com|\.de|\.fr|\.co\.uk|\.net|\.org|\.| |bugzilla'
                 '|bugs|bug|gmail|yahoo|mozilla|gentoo')
MAIL_PAT = re.compile('<([^>]*)>')


def normalize(text, ngram=3):
    text = text.lower()
    text = PAT.sub('', text)
    toks = [''.join(n) for n in ngrams(text, ngram)]
    return toks


def cosine(t1, t2):
    vectorizer = TfidfVectorizer(tokenizer=normalize)
    tfidf = vectorizer.fit_transform([t1, t2])
    return ((tfidf * tfidf.T).A)[0, 1]


def update(author_to_bz, res):
    for author, name in res.items():
        author_to_bz[author] = name


def collect_bzmail_1(author_to_bz, authors):
    # we can expect that an author is also the bug assignee
    # so we get the bugzilla email which is the more used
    # as assignee for an author
    res = {}
    for author, info in authors.items():
        assignees = info['assignees']
        if assignees:
            main_assignee, M = max(assignees.items(), key=lambda p: p[1])
            others = set(p[0] for p in assignees.items() if p[1] == M)
            if M >= 2:
                if len(others) == 1:
                    res[author] = main_assignee
                else:
                    attachers = set(info['attachers'].keys())
                    reviewees = set(info['reviewees'].keys())
                    s = others & attachers & reviewees
                    if len(s) == 1:
                        res[author] = list(s)[0]
            elif M == 1:
                attachers = set(info['attachers'].keys())
                reviewees = set(info['reviewees'].keys())
                s = others & attachers & reviewees
                if len(s) == 1:
                    res[author] = list(s)[0]

    for author in res.keys():
        del authors[author]
    update(author_to_bz, res)


def collect_bzmail_2(author_to_bz, authors):
    # the hg author can be "Andrew Scheff <ascheff@mozilla.com>"
    # or "brendan@mozilla.org".
    # So here get the mail from author and check if this mail is corresponding
    # to an email in assignees, attachers, reviewees or commenters
    fields = ['assignees', 'attachers', 'reviewees', 'commenters']
    res = {}
    for author, info in authors.items():
        persons = set(k for f in fields for k in info[f].keys())
        m = MAIL_PAT.search(author)
        if m:
            email = m.group(1)
            if email in persons:
                res[author] = email
        elif author in persons:
            res[author] = author

    for author in res.keys():
        del authors[author]
    update(author_to_bz, res)


def collect_bzmail_3(mailnames, author_to_bz, authors, threshold):
    # get all the people involved in the bug and try to find one where
    # the hg author is closed (according to threshold) to one
    # of this people
    fields = ['assignees', 'attachers', 'reviewees', 'commenters']
    res = {}
    for author, info in authors.items():
        persons = set(k for f in fields for k in info[f].keys())
        for p in persons:
            c = cosine(author, p)
            if c > threshold:
                res[author] = p
                break
            else:
                added = False
                for realname in mailnames.get(p, []):
                    c = cosine(author, realname)
                    if c > threshold:
                        added = True
                        res[author] = p
                        break
                if added:
                    break

    for author in res.keys():
        del authors[author]
    update(author_to_bz, res)


def compute(atb, threshold, a):
    res = {}
    for author in a:
        for k, bzname in atb.items():
            if cosine(author, k) > threshold:
                res[author] = bzname
                break
    return res


def __compute_helper(args):
    return compute(*args)


def collect_bzmail_4(author_to_bz, authors, threshold):
    def chunks(l, chunk_size):
        for i in range(0, len(l), chunk_size):
            yield l[i:(i + chunk_size)]

    all_authors = list(authors.keys())
    Np = cpu_count()
    Np = Np - 1 if Np > 1 else Np
    N = int(1 + len(all_authors) // Np)
    args = [(author_to_bz, threshold, c) for c in chunks(all_authors, N)]

    msg = 'Collect bzmail 4: {} cpus, len(chunk)={}'\
          ', len(all_authors)={}, len(author_to_bz)={}'
    msg = msg.format(Np, N, len(all_authors), len(author_to_bz))
    logging.info(msg)

    pool = Pool(processes=Np)
    results = pool.map(__compute_helper, args)
    res = {k: v for r in results for k, v in r.items()}
    for author in res.keys():
        del authors[author]
    update(author_to_bz, res)


def collect_bzmail_5(author_to_bz, authors):
    res = {}
    for author, info in authors.items():
        assignees = info['assignees']
        if len(assignees) == 1:
            res[author] = list(assignees.keys())[0]
        else:
            added = False
            attachers = info['attachers']
            reviewees = info['reviewees']
            commenters = info['commenters']
            if assignees:
                people = assignees.keys()
                cum = assignees.copy()
            else:
                people = set(attachers.keys()) | set(reviewees.keys())
                cum = {p: 0 for p in people}

            if cum:
                for a in people:
                    cum[a] += attachers.get(a, 0) + reviewees.get(a, 0)

                a, M = max(cum.items(), key=lambda p: p[1])
                others = set(p[0] for p in cum.items() if p[1] == M)
                if len(others) == 1:
                    res[author] = a
                    added = True
            else:
                others = commenters

            if not added:
                if commenters:
                    a, M = max(((o, commenters.get(o, 0)) for o in others),
                               key=lambda p: p[1])
                    res[author] = a
                else:
                    msg = 'Author {} has no commenters !'
                    msg = msg.format(author)
                    logging.info(author)

    for author in res.keys():
        del authors[author]
    update(author_to_bz, res)


def cleanup(author_to_bz, authors):
    fields = ['assignees', 'attachers', 'reviewees', 'commenters']
    bzs = set()
    for v in author_to_bz.values():
        bzs.update(v)
    for author, info in authors.items():
        for f in fields:
            for i in (set(info[f].keys()) & bzs):
                del info[f][i]


def print_res(res):
    def pr(r, f):
        print(' - {}:'.format(f))
        for k, v in r[f].items():
            print('   - {}: {}'.format(k, v))

    fields = ['assignees', 'reviewees', 'attachers', 'commenters']
    for k, v in res.items():
        print(k)
        for f in fields:
            pr(v, f)


def print_for_names(names, res):
    print_res({n: res[n] for n in names if n in res})


def get_map_hg_bz(stats):
    # make a deepcopy because we need to save the stats
    # and the entries in this dict will be deleted
    stats_by_author = deepcopy(stats['stats'])
    mailnames = stats['mailnames']
    atb = {}

    collect_bzmail_1(atb, stats_by_author)
    collect_bzmail_2(atb, stats_by_author)
    collect_bzmail_3(mailnames, atb, stats_by_author, 0.4)
    collect_bzmail_4(atb, stats_by_author, 0.4)
    collect_bzmail_5(atb, stats_by_author)

    return atb
