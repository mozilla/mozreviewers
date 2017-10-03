# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import whatthepatch
from libmozdata.hgmozilla import Annotate


def get_files(patch):
    files = {'touched': [],
             'deleted': [],
             'added': [],
             'moved': {}}

    lines = patch.split('\n')
    N = len(lines)
    for i in range(N):
        line = lines[i]
        if line.startswith('diff --git a/'):
            toks = line.split(' ')
            old_p = toks[2]
            old_p = old_p[2:] if old_p.startswith('a/') else old_p

            if lines[i + 1].startswith('deleted file'):
                files['deleted'].append(old_p)
            elif lines[i + 1].startswith('new file'):
                files['added'].append(old_p)
            else:
                new_p = toks[3]
                new_p = new_p[2:] if new_p.startswith('b/') else new_p

                if old_p != new_p:
                    files['moved'][old_p] = new_p
                else:
                    files['touched'].append(old_p)
    return files


def analyze_annotations(info, annotations):
    deleted = defaultdict(lambda: 0)
    alllines = defaultdict(lambda: 0)
    for path, rmed in info.items():
        annotation = annotations[path]['annotate']
        for line in rmed:
            last_author = annotation[line - 1]['author']
            deleted[last_author] += 1
    for annotation in annotations.values():
        for a in annotation['annotate']:
            last_author = a['author']
            alllines[last_author] += 1

    return {'deleted': deleted,
            'all': alllines}


def analyze_patch(patch, check_annotations):
    files = get_files(patch)
    changed = set(files['touched']) | set(files['moved'].keys())

    if check_annotations:
        newed = set(files['added']) | set(files['deleted'])

        info = defaultdict(lambda: [])
        for diff in whatthepatch.parse_patch(patch):
            h = diff.header
            if not h:
                continue

            old_p = h.old_path
            old_p = old_p[2:] if old_p.startswith('a/') else old_p
            if old_p in newed:
                # the file has just been added or deleted,
                # so nothing to compute
                continue

            for old, new, _ in diff.changes:
                if old is not None and new is None:
                    # removed line
                    info[old_p].append(old)

        files = list(info.keys())
        if files:
            annotations = Annotate.get(files, node='tip')
            stats = analyze_annotations(info, annotations)
            return stats, changed

    return {'deleted': {}, 'all': {}}, changed
