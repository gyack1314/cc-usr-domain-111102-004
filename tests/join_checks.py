'''流式外连接的共享检查：朴素袋连接参照与结果归一化。'''

from collections import defaultdict

from workbench.domain import typed_key
from workbench.join import JoinedRow


def _key_of(row, columns):
    key = typed_key(row, columns)
    return None if any(part is None for part in key) else key


def naive_full_outer_join(left_rows, right_rows, columns):
    '''小数据朴素实现：dict袋 + 完整笛卡尔积，作为流式结果的逐项对照。'''
    bags = defaultdict(lambda: ([], []))
    left_nulls = []
    right_nulls = []
    for ordinal, row in enumerate(left_rows):
        key = _key_of(row, columns)
        if key is None:
            left_nulls.append((ordinal, row))
        else:
            bags[key][0].append(row)
    for ordinal, row in enumerate(right_rows):
        key = _key_of(row, columns)
        if key is None:
            right_nulls.append((ordinal, row))
        else:
            bags[key][1].append(row)

    out = []
    for key in sorted(bags):
        left, right = bags[key]
        left = sorted(left, key=lambda r: r['source_row_id'])
        right = sorted(right, key=lambda r: r['source_row_id'])
        if left and right:
            for lrow in left:
                for rrow in right:
                    out.append(JoinedRow(key, lrow, rrow, True, True,
                                         lrow['source_row_id'],
                                         rrow['source_row_id']))
        elif left:
            for row in left:
                out.append(JoinedRow(key, row, None, True, False,
                                     row['source_row_id'], None))
        else:
            for row in right:
                out.append(JoinedRow(key, None, row, False, True,
                                     None, row['source_row_id']))
    # null未匹配：来源L先R后，各自按输入行号（同来源ID已唯一）。
    for ordinal, row in sorted(left_nulls):
        out.append(JoinedRow(None, row, None, True, False,
                             row['source_row_id'], None))
    for ordinal, row in sorted(right_nulls):
        out.append(JoinedRow(None, None, row, False, True,
                             None, row['source_row_id']))
    return out


def normalize(stream):
    return [item.as_dict() for item in stream]


def expected(left_rows, right_rows, columns):
    return normalize(
        naive_full_outer_join(list(left_rows), list(right_rows), columns))


COLUMNS = {'store': 'string', 'task': 'integer'}


def assert_join_matches_naive(case, left, right, columns=COLUMNS, **kwargs):
    # 两侧都包成独立list：朴素实现与流式实现各消费一份，流式输入仍用Once。
    left_data = list(left)
    right_data = list(right)
    from workbench.domain import Once
    from workbench.join import full_outer_join
    stream = full_outer_join(Once(iter(left_data)), Once(iter(right_data)),
                             columns, **kwargs)
    got = normalize(stream)
    want = expected(left_data, right_data, columns)
    case.assertEqual(got, want)
    return got, stream
