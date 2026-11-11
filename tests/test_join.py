import os
import tempfile
import unittest
from pathlib import Path

from workbench.domain import (Once, big_group_rows, order_rows,
                              stream_jsonl, telemetry_rows)
from workbench.io import canonical, read_json
from workbench.join import (BudgetExceededError, FileLimiter, JoinError,
                            MemoryMeter, SpillCorruptError,
                            _Record, _SpillReader, _SpillWriter,
                            _record_to_envelope, full_outer_join)
from tests.join_checks import COLUMNS, assert_join_matches_naive, expected

ROOT = Path(__file__).resolve().parents[1]


def sample_rows(name):
    return read_json(ROOT / 'samples' / name)


class JoinCorrectnessTests(unittest.TestCase):
    def test_samples_end_to_end(self):
        left = sample_rows('left.json')
        right = sample_rows('right.json')
        got, stream = assert_join_matches_naive(
            self, left, right, budget_bytes=4096, open_files=6)
        stats = stream.stats
        self.assertEqual(stats.left_rows, 4)
        self.assertEqual(stats.right_rows, 4)
        # (S1,1): 2x2=4对；S3、S4各一条未匹配；两条null未匹配。
        self.assertEqual(stats.matched_pairs, 4)
        self.assertEqual(stats.left_unmatched, 2)
        self.assertEqual(stats.right_unmatched, 2)
        present = [(r['left_present'], r['right_present']) for r in got]
        self.assertIn((True, True), present)
        self.assertEqual(present.count((True, False)), 2)
        self.assertEqual(present.count((False, True)), 2)
        # null未匹配固定在最后，L来源先于R来源。
        self.assertIsNone(got[-1]['key'])
        self.assertIsNone(got[-2]['key'])
        self.assertTrue(got[-2]['left_present'])
        self.assertTrue(got[-1]['right_present'])

    def test_fixed_output_order(self):
        left = [
            {'source_row_id': 'L9', 'store': 'B', 'task': 2, 'v': None},
            {'source_row_id': 'L1', 'store': 'A', 'task': 1, 'v': None},
            {'source_row_id': 'L2', 'store': 'A', 'task': 1, 'v': None},
        ]
        right = [
            {'source_row_id': 'R8', 'store': 'A', 'task': 1, 'v': None},
            {'source_row_id': 'R1', 'store': 'A', 'task': 1, 'v': None},
            {'source_row_id': 'R9', 'store': 'C', 'task': 3, 'v': None},
        ]
        got, _ = assert_join_matches_naive(self, left, right,
                                           budget_bytes=4096)
        keys = [tuple(r['key']) if r['key'] else None for r in got]
        self.assertEqual(keys[:5], [('A', 1)] * 4 + [('B', 2)])
        matched = [r for r in got if r['left_present'] and r['right_present']]
        self.assertEqual([(r['left_source_row_id'], r['right_source_row_id'])
                          for r in matched],
                         [('L1', 'R1'), ('L1', 'R8'),
                          ('L2', 'R1'), ('L2', 'R8')])
        self.assertEqual(keys[-1], ('C', 3))

    def test_null_never_matches_even_both_null(self):
        left = [{'source_row_id': 'L1', 'store': 'S1', 'task': None},
                {'source_row_id': 'L2', 'store': 'S1', 'task': None}]
        right = [{'source_row_id': 'R1', 'store': 'S1', 'task': None}]
        got, stats_holder = assert_join_matches_naive(
            self, left, right, budget_bytes=4096)
        self.assertEqual(stats_holder.stats.matched_pairs, 0)
        null_rows = [r for r in got if r['key'] is None]
        self.assertEqual(len(null_rows), 3)
        self.assertEqual([r['left_present'] for r in null_rows],
                         [True, True, False])
        # null在两侧spill，必有spill读写。
        self.assertGreater(stats_holder.stats.spill_bytes_written, 0)
        self.assertGreater(stats_holder.stats.spill_bytes_read, 0)

    def test_composite_null_any_column_null_never_matches(self):
        # 三列复合键：store为null或task为null都使整键不参与匹配，
        # 即使两侧在null列上“看起来相同”。
        cols = {'store': 'string', 'task': 'integer', 'code': 'integer'}
        left = [
            {'source_row_id': 'L1', 'store': None, 'task': 1, 'code': 1},
            {'source_row_id': 'L2', 'store': 'A', 'task': None, 'code': 1},
            {'source_row_id': 'L3', 'store': 'A', 'task': 1, 'code': 1},
            {'source_row_id': 'L4', 'store': 'A', 'task': 1, 'code': 2},
        ]
        right = [
            {'source_row_id': 'R1', 'store': None, 'task': 1, 'code': 1},
            {'source_row_id': 'R2', 'store': 'A', 'task': None, 'code': 1},
            {'source_row_id': 'R3', 'store': 'A', 'task': 1, 'code': 1},
        ]
        got, stream = assert_join_matches_naive(
            self, left, right, cols, budget_bytes=2000)
        matched = [r for r in got
                   if r['left_present'] and r['right_present']]
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0]['left_source_row_id'], 'L3')
        # null未匹配固定在末尾，来源L(L1,L2)先于R(R1,R2)。
        nulls = [r for r in got if r['key'] is None]
        self.assertEqual(
            [r['left_source_row_id'] or r['right_source_row_id']
             for r in nulls], ['L1', 'L2', 'R1', 'R2'])
        self.assertEqual(stream.stats.matched_pairs, 1)

    def test_strict_types_and_unicode_ordering(self):
        # 字符串'1'不等于整数1；按Unicode码点，大写A(65)在小写a(97)前。
        left = [
            {'source_row_id': 'L1', 'store': '1', 'task': 1},
            {'source_row_id': 'L2', 'store': 'A', 'task': 1},
            {'source_row_id': 'L3', 'store': 'a', 'task': 1},
            {'source_row_id': 'L4', 'store': '甲', 'task': 1},
        ]
        # 整数store在integer列下合法，但与字符串键永不相等。
        right_int = [
            {'source_row_id': 'R1', 'store': 1, 'task': 1},
        ]
        with self.assertRaises(ValueError):
            # string列遇到整数store：类型精确匹配，拒绝。
            list(full_outer_join(iter(left), iter(right_int), COLUMNS,
                                 budget_bytes=4096))
        right = [
            {'source_row_id': 'R2', 'store': 'a', 'task': 1},
            {'source_row_id': 'R3', 'store': 'A', 'task': 1},
        ]
        cols = {'store': 'string', 'task': 'integer'}
        got, _ = assert_join_matches_naive(self, left, right, cols,
                                           budget_bytes=4096)
        order = [r['key'][0] for r in got
                 if r['key'] is not None and r['left_present']
                 and r['right_present']]
        self.assertEqual(order, ['A', 'a'])
        # 字符串'1'与'甲'在左侧未匹配（右侧无对应字符串键）。
        self.assertEqual(
            sorted(r['key'][0] for r in got
                   if r['key'] is not None
                   and not (r['left_present'] and r['right_present'])),
            ['1', '甲'])

    def test_bool_is_not_integer(self):
        rows = [{'source_row_id': 'L1', 'store': 'S', 'task': True}]
        with self.assertRaises(ValueError):
            list(full_outer_join(iter(rows), iter([]), COLUMNS,
                                 budget_bytes=4096))

    def test_real_null_field_distinct_from_missing_side(self):
        left = [{'source_row_id': 'L1', 'store': 'S', 'task': 1, 'v': None}]
        right = [{'source_row_id': 'R1', 'store': 'S', 'task': 1, 'w': None}]
        got, _ = assert_join_matches_naive(self, left, right,
                                           budget_bytes=4096)
        match = got[0]
        self.assertTrue(match['left_present'] and match['right_present'])
        self.assertIsNone(match['left']['v'])  # 真实空字段仍在行内
        self.assertIsNone(match['right']['w'])
        unmatched = [r for r in got if not (r['left_present']
                                             and r['right_present'])]
        # 缺行一侧整个对象为None，与真实null字段可区分。
        for row in unmatched:
            missing_side = row['right'] if row['left_present'] else row['left']
            self.assertIsNone(missing_side)

    def test_empty_sides(self):
        got, stream = assert_join_matches_naive(
            self, [], [{'source_row_id': 'R1', 'store': 'S', 'task': 1}],
            budget_bytes=4096)
        self.assertEqual(len(got), 1)
        got, stream = assert_join_matches_naive(self, [], [],
                                                budget_bytes=4096)
        self.assertEqual(got, [])
        self.assertEqual(stream.stats.matched_pairs, 0)


class SpillAndBudgetTests(unittest.TestCase):
    def test_tiny_budget_forces_many_segments_and_merge_rounds(self):
        left = list(telemetry_rows(60, dup_every=9))
        right = list(order_rows(50, dup_every=7))
        got, stream = assert_join_matches_naive(
            self, left, right, budget_bytes=1500, open_files=4)
        stats = stream.stats
        self.assertGreaterEqual(stats.spill_segments, 4)
        self.assertGreaterEqual(stats.merge_passes, 1)
        # open_files=4时连接fan-in为1，归并必然经过多轮。
        self.assertLessEqual(stats.open_files_peak, 4)
        self.assertGreater(stats.spill_bytes_read, 0)
        # 全部临时文件 = 排序/归并/审计段 + 每侧至多一个null文件（本用例无大组）。
        null_files = (1 if any(r['task'] is None for r in left) else 0) \
            + (1 if any(r['task'] is None for r in right) else 0)
        self.assertEqual(stats.spill_files_created,
                         stats.spill_segments + null_files)
        self.assertGreaterEqual(stats.spill_files_created, stats.spill_segments)

    def test_open_files_cap_holds_across_merges(self):
        left = list(telemetry_rows(120, dup_every=5))
        right = list(order_rows(100, dup_every=6))
        for cap in (4, 5, 6, 8):
            with self.subTest(cap=cap):
                got, stream = assert_join_matches_naive(
                    self, left, right, budget_bytes=1500, open_files=cap)
                self.assertLessEqual(stream.stats.open_files_peak, cap)

    def test_single_row_over_budget_is_rejected(self):
        row = {'source_row_id': 'L1', 'store': 'S', 'task': 1,
               'big': 'x' * 500}
        cost = len(canonical(row)) + 64
        with self.assertRaises(BudgetExceededError):
            list(full_outer_join(iter([row]), iter([]), COLUMNS,
                                 budget_bytes=cost - 1))
        # 恰好等于预算可以通过。
        stream = full_outer_join(iter([row]), iter([]), COLUMNS,
                                 budget_bytes=cost + 20)
        self.assertEqual(len(list(stream)), 1)

    def test_tiny_budget_rejects_without_dedup_or_silent_drop(self):
        # 预算小到连归并最低两路都放不下时明确拒绝，而不是去重或截断。
        rows = [{'source_row_id': f'L{i}', 'store': 'S', 'task': i}
                for i in range(50)]
        with self.assertRaises(BudgetExceededError):
            list(full_outer_join(iter(rows), iter([]), COLUMNS,
                                 budget_bytes=200))

    def test_oversized_row_leaves_no_meter_residue(self):
        # 超预算行在ID登记之前即拒绝，meter不留任何账，临时目录被清理。
        row = {'source_row_id': 'L1', 'store': 'S', 'task': 1,
               'big': 'y' * 400}
        cost = len(canonical(row)) + 64
        stream = full_outer_join(iter([row]), iter([]), COLUMNS,
                                 budget_bytes=cost - 1)
        with self.assertRaises(BudgetExceededError):
            list(stream)
        self.assertEqual(stream.meter.used, 0)
        self.assertFalse(os.path.exists(stream._tmpdir))

    def test_big_single_key_group_spills_and_keeps_cartesian_product(self):
        left = list(big_group_rows('L', 40))
        right = list(big_group_rows('R', 35))
        got, stream = assert_join_matches_naive(
            self, left, right, budget_bytes=1500, open_files=6)
        stats = stream.stats
        self.assertGreaterEqual(stats.big_groups_spilled, 1)
        self.assertEqual(stats.matched_pairs, 40 * 35)
        # 输出顺序：左rid升序，键内右rid升序。
        matched = [r for r in got if r['left_present'] and r['right_present']]
        self.assertEqual(matched[0]['left_source_row_id'], 'L00000000')
        self.assertEqual(matched[0]['right_source_row_id'], 'R00000000')
        self.assertEqual(matched[34]['right_source_row_id'], 'R00000034')
        self.assertEqual(matched[35]['left_source_row_id'], 'L00000001')

    def test_big_group_one_sided_unmatched(self):
        left = list(big_group_rows('L', 30, key_task=5))
        right = list(big_group_rows('R', 3, key_task=6))
        got, stream = assert_join_matches_naive(
            self, left, right, budget_bytes=1500, open_files=6)
        self.assertEqual(stream.stats.left_unmatched, 30)
        self.assertEqual(stream.stats.right_unmatched, 3)
        self.assertEqual(stream.stats.matched_pairs, 0)

    def test_big_group_left_spill_right_memory(self):
        left = list(big_group_rows('L', 50, key_task=7))
        right = list(big_group_rows('R', 2, key_task=7))
        got, stream = assert_join_matches_naive(
            self, left, right, budget_bytes=1500, open_files=6)
        self.assertEqual(stream.stats.matched_pairs, 100)

    def test_peak_memory_never_exceeds_budget(self):
        left = list(telemetry_rows(80, dup_every=4))
        right = list(order_rows(70, dup_every=5))
        meter = MemoryMeter(1500)
        stream = full_outer_join(Once(iter(left)), Once(iter(right)),
                                 COLUMNS, meter=meter, open_files=5)
        list(stream)
        self.assertLessEqual(meter.peak, meter.budget)
        self.assertEqual(meter.used, 0)  # 全部账释放归零

    def test_injected_meter_is_used(self):
        class RecordingMeter:
            def __init__(self, budget):
                self.budget = budget
                self.used = 0
                self.peak = 0
                self.events = []

            def charge(self, amount):
                self.used += amount
                self.peak = max(self.peak, self.used)
                self.events.append(('charge', amount, self.used))
                if self.used > self.budget:
                    raise BudgetExceededError('注入计量器拒绝')

            def release(self, amount):
                self.used -= amount
                self.events.append(('release', amount, self.used))

        meter = RecordingMeter(1500)
        left = list(telemetry_rows(20))
        right = list(order_rows(15))
        stream = full_outer_join(Once(iter(left)), Once(iter(right)),
                                 COLUMNS, meter=meter, open_files=6)
        list(stream)
        self.assertTrue(meter.events)
        self.assertEqual(meter.used, 0)
        self.assertLessEqual(meter.peak, 1500)


class IteratorBoundaryTests(unittest.TestCase):
    def test_input_consumed_exactly_once(self):
        left = Once(iter(list(telemetry_rows(20))))
        right = Once(iter(list(order_rows(18))))
        stream = full_outer_join(left, right, COLUMNS,
                                 budget_bytes=1500, open_files=4)
        list(stream)
        self.assertTrue(left.used and right.used)
        with self.assertRaises(RuntimeError):
            list(left)
        with self.assertRaises(RuntimeError):
            list(right)

    def test_result_is_single_pass_iterator(self):
        stream = full_outer_join(iter(list(telemetry_rows(10))),
                                 iter(list(order_rows(9))), COLUMNS,
                                 budget_bytes=1500)
        first = list(stream)
        self.assertTrue(first)
        with self.assertRaises(RuntimeError):
            iter(stream)

    def test_early_close_cleans_temp_and_stops_output(self):
        # 有界外部排序需在首个结果前完整摄入两侧（单次遍历合同），因此提前关闭
        # 的保证是：停止产出后续结果、立即清理全部临时资料且可幂等重复关闭。
        produced = {'n': 0}

        def rows():
            for i in range(100):
                yield {'source_row_id': f'L{i}', 'store': 'S', 'task': i}

        stream = full_outer_join(rows(), iter([]), COLUMNS,
                                 budget_bytes=1500, open_files=4)
        iterator = iter(stream)
        for _ in range(3):
            next(iterator)
            produced['n'] += 1
        tmpdir = stream._tmpdir
        self.assertTrue(os.path.isdir(tmpdir))
        stream.close()
        self.assertFalse(os.path.exists(tmpdir))
        # 关闭后再取结果立即结束，不会再产出新行。
        with self.assertRaises(StopIteration):
            next(iterator)
        stream.close()  # 幂等
        self.assertEqual(produced['n'], 3)

    def test_close_during_ingest_cleans_temp(self):
        # 摄入尚未开始（未调用next）就关闭：目录与可能的段都被清理。
        def rows():
            for i in range(20):
                yield {'source_row_id': f'L{i}', 'store': 'S', 'task': i}

        stream = full_outer_join(rows(), iter([]), COLUMNS,
                                 budget_bytes=1500, open_files=4)
        tmpdir = stream._tmpdir
        self.assertTrue(os.path.isdir(tmpdir))
        stream.close()
        self.assertFalse(os.path.exists(tmpdir))
        self.assertEqual(stream.meter.used, 0)

    def test_context_manager_cleans_up(self):
        stream = full_outer_join(iter(list(telemetry_rows(15))),
                                 iter(list(order_rows(12))), COLUMNS,
                                 budget_bytes=1500)
        with stream as handle:
            self.assertEqual(len(list(handle)),
                             len(expected(list(telemetry_rows(15)),
                                          list(order_rows(12)), COLUMNS)))
        self.assertFalse(os.path.exists(stream._tmpdir))

    def test_never_iterated_stream_cleans_tmpdir(self):
        stream = full_outer_join(iter(list(telemetry_rows(5))), iter([]),
                                 COLUMNS, budget_bytes=1500)
        tmpdir = stream._tmpdir
        self.assertTrue(os.path.isdir(tmpdir))  # 构造即建目录
        stream.close()
        self.assertFalse(os.path.exists(tmpdir))

    def test_duplicate_source_row_id_rejected(self):
        left = [{'source_row_id': 'X', 'store': 'S', 'task': 1},
                {'source_row_id': 'X', 'store': 'S', 'task': 2}]
        with self.assertRaises(JoinError):
            list(full_outer_join(iter(left), iter([]), COLUMNS,
                                 budget_bytes=4096))
        right = [{'source_row_id': 'X', 'store': 'S', 'task': 1}]
        with self.assertRaises(JoinError):
            list(full_outer_join(
                iter([{'source_row_id': 'X', 'store': 'S', 'task': 1}]),
                iter(right), COLUMNS, budget_bytes=4096))

    def test_duplicate_id_detected_through_spilled_audit(self):
        # 小预算强制ID审计分段spill后归并，重复仍须在归并相邻比较时被检出。
        left = [{'source_row_id': f'L{i}', 'store': 'S', 'task': i}
                for i in range(40)]
        right = [{'source_row_id': 'L7', 'store': 'S', 'task': 7}]  # 与左同源ID撞
        stream = full_outer_join(Once(iter(left)), Once(iter(right)),
                                 COLUMNS, budget_bytes=1500, open_files=4)
        with self.assertRaises(JoinError):
            list(stream)
        self.assertEqual(stream.meter.used, 0)
        self.assertFalse(os.path.exists(stream._tmpdir))

    def test_missing_key_column_is_value_error(self):
        with self.assertRaises(ValueError):
            list(full_outer_join(
                iter([{'source_row_id': 'L1', 'store': 'S'}]), iter([]),
                COLUMNS, budget_bytes=4096))


class CorruptionTests(unittest.TestCase):
    def _build_and_corrupt(self, mutate, prefix='L-null'):
        left = list(telemetry_rows(40, dup_every=6))
        right = list(order_rows(30, dup_every=5))
        stream = full_outer_join(Once(iter(left)), Once(iter(right)),
                                 COLUMNS, budget_bytes=1500, open_files=4)
        iterator = iter(stream)
        # 摄入与首个连接行在首次next完成；null未匹配段排在结果最后，
        # 其读取器在后续迭代才打开，因此此刻损坏它必定被逐帧校验捕获。
        next(iterator)
        candidates = [
            os.path.join(stream._tmpdir, name)
            for name in os.listdir(stream._tmpdir)
            if name.startswith(prefix) and name.endswith('.spill')]
        self.assertTrue(candidates)
        target = candidates[0]
        with open(target, 'rb') as handle:
            data = bytearray(handle.read())
        mutate(data)
        with open(target, 'wb') as handle:
            handle.write(data)
        return stream, iterator

    def test_corrupt_payload_raises_not_silent_missing(self):
        def flip(data):
            # 偏移40落在首帧payload内（7字节头+4字节长度之后），
            # 翻转必破坏该帧sha256或段尾整体摘要。
            data[40] ^= 0xFF
        stream, iterator = self._build_and_corrupt(flip)
        with self.assertRaises(SpillCorruptError):
            list(iterator)
        self.assertFalse(os.path.exists(stream._tmpdir))

    def test_truncated_segment_raises(self):
        def truncate(data):
            del data[-60:]
        stream, iterator = self._build_and_corrupt(truncate)
        with self.assertRaises(SpillCorruptError):
            list(iterator)

    def test_trailing_garbage_raises(self):
        def append_garbage(data):
            data.extend(b'garbage')
        stream, iterator = self._build_and_corrupt(append_garbage)
        with self.assertRaises(SpillCorruptError):
            list(iterator)

    def test_cleanup_on_ingest_error(self):
        def bad_rows():
            yield {'source_row_id': 'L1', 'store': 'S', 'task': 1}
            yield {'source_row_id': 'L2', 'store': 'S', 'task': True}

        stream = full_outer_join(bad_rows(), iter([]), COLUMNS,
                                 budget_bytes=1500)
        with self.assertRaises(ValueError):
            list(stream)
        self.assertFalse(os.path.exists(stream._tmpdir))
        self.assertEqual(stream.meter.used, 0)


class SpillFrameTests(unittest.TestCase):
    '''直接验证spill帧的长度与sha256校验，损坏绝不静默变成缺行。'''

    def _write_segment(self, directory):
        from workbench.join import JoinStats
        stats = JoinStats()
        limiter = FileLimiter(8, stats)
        path = os.path.join(directory, 'frame.spill')
        writer = _SpillWriter(path, limiter, stats)
        for i in range(3):
            record = _Record({'i': i}, ('k', i), f'ID{i}', i, 100, False)
            writer.write(_record_to_envelope(record))
        writer.close()
        return path, limiter, stats

    def test_roundtrip_and_checksum_detects_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            path, limiter, stats = self._write_segment(directory)
            reader = _SpillReader(path, limiter, stats)
            self.assertEqual([r.rid for r in reader.records()],
                             ['ID0', 'ID1', 'ID2'])
            # 翻转payload区一字节：帧sha256必失败。
            data = bytearray(open(path, 'rb').read())
            data[20] ^= 0x01
            open(path, 'wb').write(data)
            reader = _SpillReader(path, limiter, stats)
            with self.assertRaises(SpillCorruptError):
                list(reader.records())
            reader.close()
            self.assertEqual(limiter.open, 0)

    def test_truncation_and_bad_header_raise(self):
        with tempfile.TemporaryDirectory() as directory:
            path, limiter, stats = self._write_segment(directory)
            raw = bytearray(open(path, 'rb').read())
            open(path, 'wb').write(raw[:-12])
            reader = _SpillReader(path, limiter, stats)
            with self.assertRaises(SpillCorruptError):
                list(reader.records())
            reader.close()
            # 段头损坏：构造读取器时即拒绝。
            open(path, 'wb').write(b'XX' + bytes(raw[2:]))
            with self.assertRaises(SpillCorruptError):
                _SpillReader(path, limiter, stats)
            self.assertEqual(limiter.open, 0)


class JsonlBoundaryTests(unittest.TestCase):
    def test_stream_jsonl_inputs_single_pass(self):
        left = Once(stream_jsonl(ROOT / 'samples' / 'left.jsonl'))
        right = Once(stream_jsonl(ROOT / 'samples' / 'right.jsonl'))
        stream = full_outer_join(left, right, COLUMNS,
                                 budget_bytes=1500, open_files=6)
        got = [r.as_dict() for r in stream]
        self.assertEqual(got, expected(sample_rows('left.json'),
                                       sample_rows('right.json'), COLUMNS))
        with self.assertRaises(RuntimeError):
            list(left)


if __name__ == '__main__':
    unittest.main()
