'''有界内存的流式 full outer join。

输入是只能遍历一次的行迭代器，输出同样是迭代器。两侧各自先做外部排序
（内存内排序段 + 多轮归并，全部spill在同一内存预算内计量），再归并连接：

* 键由配置列定义，类型必须精确匹配；复合键任一列为null则不参与匹配。
* 非null键的重复组输出完整笛卡尔积，重复行不做去重。
* 未匹配行以另一侧为None的配对输出，真实字段为None与“缺行”用
  left_present/right_present标志区分。
* 单键组大于预算时转为spill上的块嵌套循环，不退回整组list。
* 每个spill段带帧长度与sha256校验，段尾带摘要；损坏显式报错，
  不会被当成缺行吞掉。
* 正常耗尽、异常、调用者提前关闭都会清理本次全部临时资料。

内存账是显式的逻辑上界（不声称等于RSS）。每行保留成本为canonical JSON
编码字节数加固定记录开销；排序缓冲、归并首行、重复组缓存、块嵌套循环
切块均计入同一预算。source_row_id唯一性同样不使用全局内存集合，而是
按 ID_ENTRY_OVERHEAD 加ID的UTF-8字节数逐笔记账，经独立的有界外部审计
（spill后按ID归并）检测重复，因此输入规模再大也不会因ID表撑爆预算。
'''

import contextlib
import hashlib
import heapq
import json
import os
import tempfile
from collections import deque

from .io import canonical
from .domain import typed_key

# spill文件魔数与尾块；帧长度高位置0，尾块长度高位置1以区分。
_HEADER = b'WBJOIN1'
_FOOTER_MAGIC = b'WBEND1\x00'
_FOOTER_TAG = 1 << 31

# 唯一性集合中每个ID条目的逻辑记账开销（ID的UTF-8字节数另计）。
ID_ENTRY_OVERHEAD = 8


class JoinError(RuntimeError):
    '''连接过程中的确定性错误（损坏段、重复ID等）。'''


class SpillCorruptError(JoinError):
    '''临时段长度或校验不符；损坏绝不静默变成缺行。'''


class BudgetExceededError(MemoryError):
    '''单行超预算，或预算无法容纳必要的同时驻留记录。'''


class MemoryMeter:
    '''可注入的逻辑内存计量器：只记保留中的行账，峰值取历史最大。'''

    def __init__(self, budget_bytes):
        if budget_bytes < 0:
            raise ValueError('预算不能为负')
        self.budget = int(budget_bytes)
        self.used = 0
        self.peak = 0

    def charge(self, amount):
        amount = int(amount)
        if amount > self.budget:
            raise BudgetExceededError(
                f'单行计量{amount}字节超过预算{self.budget}字节')
        if self.used + amount > self.budget:
            raise BudgetExceededError(
                f'内存预算超限：已用{self.used}+申请{amount}>{self.budget}')
        self.used += amount
        if self.used > self.peak:
            self.peak = self.used

    def release(self, amount):
        amount = int(amount)
        if amount > self.used:
            raise JoinError('内存计量释放超过保留值')
        self.used -= amount


class JoinStats:
    '''真实运行的观察口径；spill字节数按spill帧payload累计。'''

    __slots__ = ('left_rows', 'right_rows', 'matched_pairs',
                 'left_unmatched', 'right_unmatched',
                 'spill_segments', 'spill_files_created',
                 'spill_bytes_written', 'spill_bytes_read',
                 'merge_passes', 'big_groups_spilled', 'open_files_peak')

    def __init__(self):
        for name in self.__slots__:
            setattr(self, name, 0)

    def as_dict(self):
        return {name: getattr(self, name) for name in self.__slots__}


class FileLimiter:
    '''组件自身打开文件描述符的硬上限（调用者的输入迭代器不计）。'''

    def __init__(self, limit, stats):
        if limit < 4:
            raise ValueError('open_files至少为4（两个spill读、一个写加余量）')
        self.limit = limit
        self.open = 0
        self.stats = stats

    def acquire(self):
        if self.open >= self.limit:
            raise JoinError(f'打开文件数超过上限{self.limit}')
        self.open += 1
        if self.open > self.stats.open_files_peak:
            self.stats.open_files_peak = self.open

    def release(self):
        if self.open <= 0:
            raise JoinError('文件描述符计数不平衡')
        self.open -= 1


class _Record:
    '''排序与连接管线中的一行；内存账按cost计量。'''

    __slots__ = ('row', 'key', 'rid', 'ordinal', 'cost', 'has_null')

    def __init__(self, row, key, rid, ordinal, cost, has_null):
        self.row = row
        self.key = key
        self.rid = rid
        self.ordinal = ordinal
        self.cost = cost
        self.has_null = has_null


def _key_sort_key(record):
    return (record.key, record.rid)


class _SpillWriter:
    '''spill段：帧 = uint32长度 + payload + sha256(payload)，段尾附摘要。'''

    def __init__(self, path, limiter, stats):
        self.path = path
        self.limiter = limiter
        self.stats = stats
        self.count = 0
        self.digest = hashlib.sha256()
        self.stream = None
        limiter.acquire()
        try:
            self.stream = open(path, 'wb')
            self.stream.write(_HEADER)
        except BaseException:
            self._safe_release()
            raise

    def _safe_release(self):
        if self.stream is not None:
            try:
                self.stream.close()
            finally:
                self.stream = None
                self.limiter.release()
        else:
            self.limiter.release()

    def write(self, envelope):
        payload = canonical(envelope)
        frame = len(payload).to_bytes(4, 'big') + payload \
            + hashlib.sha256(payload).digest()
        self.stream.write(frame)
        self.digest.update(payload)
        self.count += 1
        self.stats.spill_bytes_written += len(payload)

    def close(self):
        if self.stream is None:
            return
        stream = self.stream
        self.stream = None
        try:
            footer = _FOOTER_TAG.to_bytes(4, 'big') + _FOOTER_MAGIC \
                + self.count.to_bytes(8, 'big') + self.digest.digest()
            stream.write(footer)
            stream.flush()
        finally:
            stream.close()
            self.limiter.release()


class _SpillReader:
    '''逐帧校验读取；任何截断、长度或摘要不符都抛SpillCorruptError。'''

    def __init__(self, path, limiter, stats, parser=None):
        self.path = path
        self.limiter = limiter
        self.stats = stats
        self.parser = parser if parser is not None else _record_from_envelope
        self.stream = None
        self._released = False
        limiter.acquire()
        try:
            self.stream = open(path, 'rb')
            header = self._read_exact(len(_HEADER))
            if header != _HEADER:
                raise SpillCorruptError(f'spill段头损坏：{path}')
        except BaseException:
            self.close()
            raise

    def _read_exact(self, size):
        chunk = self.stream.read(size)
        if len(chunk) != size:
            raise SpillCorruptError(f'spill段截断：{self.path}')
        return chunk

    def records(self):
        '''产生未计内存账的_Record；由消费者负责charge。'''
        stream = self.stream
        total = hashlib.sha256()
        count = 0
        while True:
            prefix = stream.read(4)
            if not prefix:
                raise SpillCorruptError(f'spill段缺少尾块：{self.path}')
            length = int.from_bytes(prefix, 'big')
            if length & _FOOTER_TAG:
                magic = self._read_exact(len(_FOOTER_MAGIC))
                if magic != _FOOTER_MAGIC:
                    raise SpillCorruptError(f'spill尾块魔数损坏：{self.path}')
                expect_count = int.from_bytes(self._read_exact(8), 'big')
                expect_digest = self._read_exact(32)
                if stream.read(1):
                    raise SpillCorruptError(f'spill尾块后有多余字节：{self.path}')
                if expect_count != count or expect_digest != total.digest():
                    raise SpillCorruptError(f'spill段摘要不符：{self.path}')
                self.close()
                return
            if length == 0:
                raise SpillCorruptError(f'spill帧长度非法：{self.path}')
            payload = self._read_exact(length)
            checksum = self._read_exact(32)
            if hashlib.sha256(payload).digest() != checksum:
                raise SpillCorruptError(f'spill帧校验失败：{self.path}')
            total.update(payload)
            count += 1
            self.stats.spill_bytes_read += length
            yield self.parser(json.loads(payload.decode('utf-8')))

    def close(self):
        if self._released:
            return
        self._released = True
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        self.limiter.release()


def _record_to_envelope(record):
    return {'key': list(record.key) if record.key is not None else None,
            'rid': record.rid, 'ordinal': record.ordinal,
            'cost': record.cost, 'has_null': record.has_null,
            'row': record.row}


def _record_from_envelope(envelope):
    key = envelope['key']
    key = None if key is None else tuple(key)
    return _Record(envelope['row'], key, envelope['rid'],
                   envelope['ordinal'], envelope['cost'],
                   envelope['has_null'])


@contextlib.contextmanager
def _open_spill(path, limiter, stats):
    reader = _SpillReader(path, limiter, stats)
    try:
        yield reader
    finally:
        reader.close()


class _ListRun:
    '''内存内的已排序run；逐条释放账，关闭时释放剩余账。'''

    def __init__(self, records, meter):
        self.records = records
        self.meter = meter

    def raw(self):
        records = self.records
        meter = self.meter
        while records:
            record = records.popleft()
            meter.release(record.cost)
            yield record

    def close(self):
        meter = self.meter
        for record in self.records:
            meter.release(record.cost)
        self.records.clear()


class _SegmentRun:
    '''spill段run；raw()产生未计账的记录，关闭归还描述符。'''

    def __init__(self, path, limiter, stats):
        self.path = path
        self.limiter = limiter
        self.stats = stats
        self._reader = None

    def raw(self):
        self._reader = _SpillReader(self.path, self.limiter, self.stats)
        return self._reader.records()

    def close(self):
        if self._reader is not None:
            self._reader.close()
            self._reader = None


class _MergedRun:
    '''多个run的k路归并；raw()产生未计账记录（堆顶首行临时计账）。'''

    def __init__(self, runs, meter):
        self.runs = runs
        self.meter = meter
        self._gen = None

    def raw(self):
        self._gen = self._iterate()
        return self._gen

    def _iterate(self):
        meter = self.meter
        heap = []
        try:
            with contextlib.ExitStack() as stack:
                for run in self.runs:
                    stack.callback(run.close)
                for run_index, run in enumerate(self.runs):
                    iterator = run.raw()
                    try:
                        first = next(iterator)
                    except StopIteration:
                        continue
                    meter.charge(first.cost)
                    # run_index打破同(key,rid)平局，避免比较_Record对象。
                    heap.append((_key_sort_key(first), run_index, first, iterator))
                heapq.heapify(heap)
                while heap:
                    _, run_index, record, iterator = heapq.heappop(heap)
                    meter.release(record.cost)
                    yield record
                    try:
                        nxt = next(iterator)
                    except StopIteration:
                        continue
                    meter.charge(nxt.cost)
                    heapq.heappush(
                        heap, (_key_sort_key(nxt), run_index, nxt, iterator))
                self.runs = []
        finally:
            # 生成器被提前close时，释放堆中仍计账的首行。
            for item in heap:
                meter.release(item[2].cost)
            heap.clear()

    def close(self):
        # 先关闭归并生成器以触发ExitStack，释放堆中已计账首行与各run描述符。
        if self._gen is not None:
            self._gen.close()
            self._gen = None
        for run in self.runs:
            run.close()
        self.runs = []


class _ExternalSorter:
    '''一侧输入的外部排序：内存段 -> spill段 -> 多轮归并到单个有序run。

    本对象是摄入期唯一的刷盘协调者：ID审计器不自行判断预算，排序缓冲
    需要刷段时同步把审计器的待审ID刷出，使两个缓冲同时清空、绝不并发堆积。
    '''

    def __init__(self, side, columns, meter, limiter, stats, tmpdir,
                 auditor=None):
        self.side = side
        self.columns = columns
        self.meter = meter
        self.limiter = limiter
        self.stats = stats
        self.tmpdir = tmpdir
        self.auditor = auditor
        self.buffer = []
        self.buffered_cost = 0
        self.segments = []
        self.max_row_cost = 0
        self._serial = 0

    def add(self, record):
        # 单一协调点：按共享meter实时占用判断，超限时先把ID审计缓冲与数据
        # 缓冲一并刷盘（两者同时清空）；刷完仍放不下则单行无法与必要驻留共存。
        if self.buffer and self.meter.used + record.cost > self.meter.budget:
            if self.auditor is not None:
                self.auditor.flush_pending()
            self._flush()
            if self.meter.used + record.cost > self.meter.budget:
                raise BudgetExceededError(
                    f'排序行计量{record.cost}无法纳入预算{self.meter.budget}')
        self.meter.charge(record.cost)
        self.buffer.append(record)
        self.buffered_cost += record.cost
        self.max_row_cost = max(self.max_row_cost, record.cost)

    def flush_buffer(self):
        '''显式刷出当前数据缓冲（供null行落盘前统一腾空间）。'''
        self._flush()

    def close(self):
        '''摄入失败时释放仍驻留在内存缓冲中的行账（已spill的账已释放）。'''
        for record in self.buffer:
            self.meter.release(record.cost)
        self.buffer.clear()
        self.buffered_cost = 0

    def _new_segment_path(self, prefix):
        self._serial += 1
        self.stats.spill_files_created += 1
        return os.path.join(self.tmpdir, f'{self.side}-{prefix}-{self._serial}.spill')

    def _flush(self):
        if not self.buffer:
            return
        self.buffer.sort(key=_key_sort_key)
        path = self._new_segment_path('seg')
        writer = _SpillWriter(path, self.limiter, self.stats)
        try:
            for record in self.buffer:
                writer.write(_record_to_envelope(record))
                self.meter.release(record.cost)
        finally:
            writer.close()
        self.buffer.clear()
        self.buffered_cost = 0
        self.segments.append(path)
        self.stats.spill_segments += 1

    def freeze(self):
        # 冻结数据段前，先把审计器残留的ID刷盘，保证摄入结束无内存ID滞留。
        if self.auditor is not None:
            self.auditor.flush_pending()
        if not self.segments:
            records = self.buffer
            # 驻留不变量：一侧的内存驻留必须给对侧的多路归并预留空间。
            # 归并收敛至少需要2个堆首行加1个落盘输出行，故预留3个最大单行；
            # 余量不足时本侧也落spill段，避免对侧归并阶段才超限。
            reserve = 3 * self.max_row_cost
            if records and self.meter.used + reserve > self.meter.budget:
                self._flush()
            else:
                records.sort(key=_key_sort_key)
                run = _ListRun(deque(records), self.meter)
                self.buffer = []
                self.buffered_cost = 0
                return run
        self._flush()
        # 最终连接阶段两侧各持有一个run，按每侧join_fan_in个打开文件预留，
        # 另留4个描述符给两个大组spill及其重扫。
        join_fan_in = max(1, (self.limiter.limit - 4) // 2)
        paths = list(self.segments)
        # 单段无需归并，直接作为run返回，不受多路归并的预算门槛约束。
        if len(paths) <= join_fan_in:
            runs = [_SegmentRun(p, self.limiter, self.stats) for p in paths]
            if len(runs) == 1:
                return runs[0]
            return _MergedRun(runs, self.meter)
        file_fan_in = max(join_fan_in, self.limiter.limit - 2)
        # 归并时堆中每个输入run驻留一首行，另有落盘输出行瞬时计账。
        # 关键：此刻对侧可能已有_ListRun整驻留，必须按meter“剩余”空间估算
        # 可并发首行数，而非总预算；fan-in至少为2才能让段数每轮收敛。
        resident = self.meter.used
        available = self.meter.budget - resident
        budget_fan_in = max(1, available // max(1, self.max_row_cost) - 1)
        if budget_fan_in < 2:
            raise BudgetExceededError(
                f'剩余预算{available}无法同时容纳归并所需的两个最大单行'
                f'（{self.max_row_cost}字节），拒绝以死循环或截断换取结果')
        merge_fan_in = max(2, min(file_fan_in, budget_fan_in))
        while len(paths) > join_fan_in:
            next_paths = []
            for start in range(0, len(paths), merge_fan_in):
                chunk = paths[start:start + merge_fan_in]
                runs = [_SegmentRun(p, self.limiter, self.stats) for p in chunk]
                out_path = self._new_segment_path('merge')
                self._merge_to_file(runs, out_path)
                next_paths.append(out_path)
            paths = next_paths
            self.stats.merge_passes += 1
        runs = [_SegmentRun(p, self.limiter, self.stats) for p in paths]
        if len(runs) == 1:
            return runs[0]
        return _MergedRun(runs, self.meter)

    def _merge_to_file(self, runs, out_path):
        '''归并并立即落盘；输入首行与落盘行的账在同一预算内对开。'''
        writer = _SpillWriter(out_path, self.limiter, self.stats)
        merged = _MergedRun(runs, self.meter)
        try:
            for record in merged.raw():
                self.meter.charge(record.cost)
                try:
                    writer.write(_record_to_envelope(record))
                finally:
                    self.meter.release(record.cost)
        finally:
            writer.close()
            merged.close()
        self.stats.spill_segments += 1


class _IdEntry:
    '''唯一性审计的轻量条目：只存source_row_id与来源侧，按rid外部归并。'''

    __slots__ = ('rid', 'side', 'cost')

    def __init__(self, rid, side, cost):
        self.rid = rid
        self.side = side
        self.cost = cost


def _id_envelope(entry):
    return {'rid': entry.rid, 'side': entry.side, 'cost': entry.cost}


def _id_from_envelope(envelope):
    return _IdEntry(envelope['rid'], envelope['side'], envelope['cost'])


class _IdAuditor:
    '''有界的source_row_id唯一性审计。

    不保留全局内存集合：条目按预算缓冲、排序后spill，审计时k路归并，
    相邻同rid即重复（含同一来源内与跨来源）。全部账计入同一内存预算，
    段文件与数据spill一样带长度和校验。
    '''

    def __init__(self, meter, limiter, stats, tmpdir):
        self.meter = meter
        self.limiter = limiter
        self.stats = stats
        self.tmpdir = tmpdir
        self.sorter = None
        self.buffer = []
        self.buffered_cost = 0
        self.max_cost = 0
        self.segments = []
        self._serial = 0

    def add(self, rid, side):
        # 不在此自行决定刷盘策略，但charge前若预算不足，先经排序器统一协调，
        # 把ID缓冲与数据缓冲一并刷盘，保证两个缓冲不并发堆积超预算。
        cost = len(rid.encode('utf-8')) + ID_ENTRY_OVERHEAD
        if cost > self.meter.budget:
            raise BudgetExceededError(
                f'ID条目计量{cost}字节超过预算{self.meter.budget}')
        self.max_cost = max(self.max_cost, cost)
        if self.buffer and self.meter.used + cost > self.meter.budget:
            if self.sorter is not None:
                self.sorter.flush_buffer()
            self.flush_pending()
        self.meter.charge(cost)
        self.buffer.append(_IdEntry(rid, side, cost))
        self.buffered_cost += cost

    def flush_pending(self):
        '''把当前待审ID刷成一个段；由排序器在刷数据段的同一时刻调用。'''
        if not self.buffer:
            return
        self._flush()

    def _new_path(self):
        self._serial += 1
        self.stats.spill_files_created += 1
        return os.path.join(self.tmpdir, f'id-seg-{self._serial}.spill')

    def _flush(self):
        if not self.buffer:
            return
        self.buffer.sort(key=lambda entry: entry.rid)
        path = self._new_path()
        writer = _SpillWriter(path, self.limiter, self.stats)
        try:
            for entry in self.buffer:
                writer.write(_id_envelope(entry))
                self.meter.release(entry.cost)
        finally:
            writer.close()
        self.buffer.clear()
        self.buffered_cost = 0
        self.segments.append(path)
        self.stats.spill_segments += 1

    def audit(self):
        '''归并全部ID条目并检测重复；审计完释放自身全部临时段。'''
        try:
            if not self.segments:
                entries = sorted(self.buffer, key=lambda e: e.rid)
                previous = None
                for entry in entries:
                    if previous is not None and entry.rid == previous:
                        raise JoinError(f'source_row_id重复：{entry.rid}')
                    previous = entry.rid
                return
            self._flush()
            file_fan_in = max(2, self.limiter.limit - 2)
            # 与数据归并同口径：按剩余预算限定可并发首行数，fan-in至少为2。
            max_cost = max(1, self.max_cost)
            available = self.meter.budget - self.meter.used
            budget_fan_in = max(1, available // max_cost - 1)
            if budget_fan_in < 2:
                raise BudgetExceededError(
                    f'剩余预算{available}无法容纳ID归并所需的两个条目'
                    f'（{max_cost}字节），拒绝以死循环换取结果')
            fan_in = max(2, min(file_fan_in, budget_fan_in))
            paths = list(self.segments)
            while len(paths) > fan_in:
                next_paths = []
                for start in range(0, len(paths), fan_in):
                    runs = [_IdSegmentRun(p, self.limiter, self.stats)
                            for p in paths[start:start + fan_in]]
                    out_path = self._new_path()
                    self._merge_to_file(runs, out_path)
                    next_paths.append(out_path)
                paths = next_paths
                self.stats.merge_passes += 1
            self._detect_duplicates(paths)
        finally:
            self.cleanup()

    def _detect_duplicates(self, paths):
        runs = [_IdSegmentRun(p, self.limiter, self.stats) for p in paths]
        merged = _IdMergedRun(runs, self.meter)
        try:
            previous = None
            for entry in merged.raw():
                if previous is not None and entry.rid == previous:
                    raise JoinError(f'source_row_id重复：{entry.rid}')
                previous = entry.rid
        finally:
            # 检出重复抛错时也要关闭归并生成器，释放堆中已计账的首行。
            merged.close()

    def _merge_to_file(self, runs, out_path):
        writer = _SpillWriter(out_path, self.limiter, self.stats)
        merged = _IdMergedRun(runs, self.meter)
        try:
            for entry in merged.raw():
                self.meter.charge(entry.cost)
                try:
                    writer.write(_id_envelope(entry))
                finally:
                    self.meter.release(entry.cost)
        finally:
            writer.close()
            merged.close()
        self.stats.spill_segments += 1

    def cleanup(self):
        for entry in self.buffer:
            self.meter.release(entry.cost)
        self.buffer.clear()
        self.buffered_cost = 0
        for path in self.segments:
            if os.path.exists(path):
                os.unlink(path)
        self.segments.clear()


class _IdSegmentRun:
    '''ID审计段的run；raw()产生未计账的_IdEntry。'''

    def __init__(self, path, limiter, stats):
        self.path = path
        self.limiter = limiter
        self.stats = stats
        self._reader = None

    def raw(self):
        self._reader = _SpillReader(self.path, self.limiter, self.stats,
                                    _id_from_envelope)
        for envelope in self._reader.records():
            yield envelope

    def close(self):
        if self._reader is not None:
            self._reader.close()
            self._reader = None


class _IdMergedRun:
    '''ID条目按rid的k路归并。'''

    def __init__(self, runs, meter):
        self.runs = runs
        self.meter = meter
        self._gen = None

    def raw(self):
        self._gen = self._iterate()
        return self._gen

    def _iterate(self):
        heap = []
        try:
            with contextlib.ExitStack() as stack:
                for run in self.runs:
                    stack.callback(run.close)
                for run_index, run in enumerate(self.runs):
                    iterator = run.raw()
                    try:
                        first = next(iterator)
                    except StopIteration:
                        continue
                    self.meter.charge(first.cost)
                    # run_index打破同rid平局，避免比较_IdEntry对象。
                    heap.append((first.rid, run_index, first, iterator))
                heapq.heapify(heap)
                while heap:
                    _, run_index, entry, iterator = heapq.heappop(heap)
                    self.meter.release(entry.cost)
                    yield entry
                    try:
                        nxt = next(iterator)
                    except StopIteration:
                        continue
                    self.meter.charge(nxt.cost)
                    heapq.heappush(heap, (nxt.rid, run_index, nxt, iterator))
                self.runs = []
        finally:
            # 生成器被提前close（如检出重复抛错）时，释放堆中仍计账的首行。
            for item in heap:
                self.meter.release(item[2].cost)
            heap.clear()

    def close(self):
        # 先关闭归并生成器以触发ExitStack，释放堆中已计账首行与各run描述符。
        if self._gen is not None:
            self._gen.close()
            self._gen = None
        for run in self.runs:
            run.close()
        self.runs = []



class _Peek:
    '''给有序run加一行回看。

    head是刚从run取出的“未计账”记录：本对象不预占内存预算，记账与溢出
    决策交给消费它的_GroupBuffer，避免连接期peek首行与大组驻留叠加超预算。
    '''

    __slots__ = ('iterator', 'head', '_closed')

    def __init__(self, run):
        self.iterator = run.raw()
        self.head = None
        self._closed = False
        self.advance()

    def take(self):
        '''取走当前head（未计账，所有权转给调用方）并拉下行。'''
        record = self.head
        self.head = None
        self.advance()
        return record

    def advance(self):
        try:
            self.head = next(self.iterator)
        except StopIteration:
            self.head = None

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.head = None
        close = getattr(self.iterator, 'close', None)
        if close is not None:
            close()


class _GroupBuffer:
    '''一个键组的单侧缓存；加行前检测预算，溢出即整体转spill。

    接收的记录来自未计账的peek head，由本对象负责charge或落spill后释放。
    '''

    def __init__(self, key, meter, limiter, stats, tmpdir, side):
        self.key = key
        self.meter = meter
        self.limiter = limiter
        self.stats = stats
        self.tmpdir = tmpdir
        self.side = side
        self.peer = None  # 同键的对侧组，用于在预算紧张时协调对侧先溢出
        self.records = []
        self.cost_total = 0
        self.max_cost = 0
        self.spill_path = None
        self._writer = None

    def _charge_transient(self, cost):
        '''瞬时计账一行（随即写盘释放）；若对侧内存组挤占导致放不下，
        先把对侧组溢出到spill腾出空间，仍放不下才明确拒绝。'''
        if self.meter.used + cost <= self.meter.budget:
            self.meter.charge(cost)
            return
        if self.peer is not None and self.peer.records:
            self.peer._spill_existing()
        if self.meter.used + cost > self.meter.budget:
            raise BudgetExceededError(
                f'键组行计量{cost}在对侧溢出后仍无法纳入预算'
                f'{self.meter.budget}，拒绝静默截断')
        self.meter.charge(cost)

    def take_from(self, peek):
        '''接收peek的未计账head：能入内存则charge，超预算则转spill并释放。'''
        record = peek.take()
        self.max_cost = max(self.max_cost, record.cost)
        # 本组已在spill：瞬时charge后立即写盘释放，不形成驻留。
        if self._writer is not None:
            self._charge_transient(record.cost)
            self._writer.write(_record_to_envelope(record))
            self.meter.release(record.cost)
            return
        # 内存路径：按meter实时占用判断（含对侧组并发驻留）。
        # 本组首行也可能因对侧占满预算而需直接转spill，故不要求已有records。
        if self.meter.used + record.cost > self.meter.budget:
            if self.records:
                self._spill_existing()
            else:
                # 本组首行就放不下：建立spill目标，后续行也走写盘路径。
                self._open_spill_writer()
            self._charge_transient(record.cost)
            self._writer.write(_record_to_envelope(record))
            self.meter.release(record.cost)
            return
        # 仍有余量：charge后驻留。单行本身超预算由meter明确拒绝。
        self.meter.charge(record.cost)
        self.records.append(record)
        self.cost_total += record.cost

    def _open_spill_writer(self):
        self.stats.big_groups_spilled += 1
        self.stats.spill_files_created += 1
        self.spill_path = os.path.join(
            self.tmpdir, f'group-{self.side}-{id(self)}.spill')
        self._writer = _SpillWriter(self.spill_path, self.limiter, self.stats)

    def _spill_existing(self):
        self._open_spill_writer()
        for item in self.records:
            self._writer.write(_record_to_envelope(item))
            self.meter.release(item.cost)
        self.records.clear()
        self.cost_total = 0

    def finish_spill(self):
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def cleanup(self):
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if self.spill_path is not None and os.path.exists(self.spill_path):
            os.unlink(self.spill_path)
        self.spill_path = None
        for record in self.records:
            self.meter.release(record.cost)
        self.records.clear()
        self.cost_total = 0

    def transient(self):
        '''逐行产出本组：内存行直接给，spill行临时计账、消费后释放。'''
        if self.spill_path is None:
            for record in self.records:
                yield record
            return
        with _open_spill(self.spill_path, self.limiter, self.stats) as reader:
            for record in reader.records():
                self.meter.charge(record.cost)
                try:
                    yield record
                finally:
                    self.meter.release(record.cost)


class JoinedRow:
    '''一条full outer join结果；present标志区分缺行与真实空字段。'''

    __slots__ = ('key', 'left', 'right', 'left_present', 'right_present',
                 'left_source_row_id', 'right_source_row_id')

    def __init__(self, key, left, right, left_present, right_present,
                 left_source_row_id, right_source_row_id):
        self.key = key
        self.left = left
        self.right = right
        self.left_present = left_present
        self.right_present = right_present
        self.left_source_row_id = left_source_row_id
        self.right_source_row_id = right_source_row_id

    def as_dict(self):
        return {
            'key': self.key,
            'left': self.left,
            'right': self.right,
            'left_present': self.left_present,
            'right_present': self.right_present,
            'left_source_row_id': self.left_source_row_id,
            'right_source_row_id': self.right_source_row_id,
        }


def _ingest(rows, side, columns, overhead, meter, limiter, stats, tmpdir,
            auditor):
    '''单次遍历一侧输入：类型检查、ID登记到有界审计、单行预算拒绝与null分流。'''
    sorter = _ExternalSorter(side, columns, meter, limiter, stats, tmpdir,
                             auditor)
    auditor.sorter = sorter
    null_writer = None
    null_path = None
    counter = 0

    def make_room(cost):
        # 为一个瞬时驻留（null行落盘）腾出空间：同时刷ID与数据缓冲。
        if meter.used + cost <= meter.budget:
            return
        auditor.flush_pending()
        sorter.flush_buffer()
        if meter.used + cost > meter.budget:
            raise BudgetExceededError(
                f'{side}侧单行计量{cost}无法纳入预算{meter.budget}')

    try:
        for ordinal, row in enumerate(rows):
            if not isinstance(row, dict):
                raise ValueError(f'{side}侧第{ordinal}行不是对象')
            rid = row.get('source_row_id')
            if not isinstance(rid, str) or not rid:
                raise ValueError(f'{side}侧source_row_id必须为非空字符串')
            try:
                key = typed_key(row, columns)
            except KeyError as exc:
                raise ValueError(f'{side}侧缺少键列：{exc.args[0]}') from exc
            has_null = any(part is None for part in key)
            cost = len(canonical(row)) + overhead
            # 单行本身超预算：在任何保留（含ID登记）之前明确拒绝。
            if cost > meter.budget:
                raise BudgetExceededError(
                    f'{side}侧第{ordinal}行计量{cost}字节超过预算{meter.budget}')
            # ID登记到有界外部审计（spill），唯一性在两侧摄入后统一归并判定。
            auditor.add(rid, side)
            record = _Record(row, key, rid, ordinal, cost, has_null)
            if has_null:
                make_room(cost)
                if null_writer is None:
                    stats.spill_files_created += 1
                    null_path = os.path.join(tmpdir, f'{side}-null.spill')
                    null_writer = _SpillWriter(null_path, limiter, stats)
                meter.charge(cost)
                try:
                    null_writer.write(_record_to_envelope(record))
                finally:
                    meter.release(cost)
            else:
                sorter.add(record)
            counter += 1
        if null_writer is not None:
            null_writer.close()
        run = sorter.freeze()
    except BaseException:
        if null_writer is not None:
            null_writer.close()
        sorter.close()
        raise
    if side == 'L':
        stats.left_rows += counter
    else:
        stats.right_rows += counter
    return run, null_path


def _emit_matched(key, left_record, right_record, stats):
    stats.matched_pairs += 1
    return JoinedRow(key, left_record.row, right_record.row, True, True,
                     left_record.rid, right_record.rid)


def _emit_unmatched(key, record, side, stats):
    if side == 'L':
        stats.left_unmatched += 1
        return JoinedRow(key, record.row, None, True, False,
                         record.rid, None)
    stats.right_unmatched += 1
    return JoinedRow(key, None, record.row, False, True,
                     None, record.rid)


def _cartesian_memory(key, left_group, right_group, stats):
    '''两侧均在内存：完整笛卡尔积，键内按左、右source_row_id输出。'''
    for left in left_group.records:
        for right in right_group.records:
            yield _emit_matched(key, left, right, stats)


def _block_nested_loops(key, left_group, right_group, meter, limiter, stats):
    '''超大键组：spill上的块嵌套循环，任何时刻驻留都不超预算。

    输出顺序固定为左rid升序、其下右rid升序，与内存笛卡尔积路径一致。
    实现以左行为外循环：每个左行（必要时按预算切块）重扫右spill，把该
    左行的配对在右流上按序产出，因此天然保持左→右顺序，无需缓存配对结果。
    预算按实际驻留记录校验；若连“左一行 + 右一行”都无法同时驻留，
    显式抛BudgetExceededError，不做静默截断。
    '''
    if left_group.spill_path is None and right_group.spill_path is None:
        yield from _cartesian_memory(key, left_group, right_group, stats)
        return

    if right_group.spill_path is None:
        # 左spill、右内存：正常时右组整驻留、左行临时计账做笛卡尔积。
        # 若一个左spill行都无法与右组同时驻留，则把右组也溢出，
        # 落到下方“右表在spill”的统一左外循环路径，保证不超限。
        if (right_group.records and right_group.cost_total
                + left_group.max_cost > meter.budget):
            right_group._spill_existing()
            right_group.finish_spill()
        else:
            right_resident = right_group.cost_total
            for left in left_group.transient():
                if right_resident + left.cost > meter.budget:
                    raise BudgetExceededError(
                        '左spill行与内存右组无法同时纳入预算，拒绝静默截断')
                for right in right_group.records:
                    yield _emit_matched(key, left, right, stats)
            return

    # 右表在spill（左表内存或spill）：左行为外循环逐个重扫右spill。
    # 每次只需“当前左行 + 右表单行”同时驻留，顺序天然为左→右。
    def left_rows():
        if left_group.spill_path is None:
            for left in left_group.records:
                yield left, False  # 内存行：账已由左组持有，无需释放
        else:
            with _open_spill(left_group.spill_path, limiter,
                             stats) as left_reader:
                for left in left_reader.records():
                    meter.charge(left.cost)
                    try:
                        yield left, True
                    finally:
                        meter.release(left.cost)

    for left, left_was_charged in left_rows():
        with _open_spill(right_group.spill_path, limiter, stats) as reader:
            for right in reader.records():
                # 直接以全局meter判定真实并发驻留；放不下显式拒绝，不静默截断。
                if meter.used + right.cost > meter.budget:
                    raise BudgetExceededError(
                        '键组左右各一行无法同时纳入预算，拒绝静默截断')
                meter.charge(right.cost)
                try:
                    yield _emit_matched(key, left, right, stats)
                finally:
                    meter.release(right.cost)


def full_outer_join(left_rows, right_rows, columns, budget_bytes=None,
                    open_files=8, record_overhead=64, meter=None,
                    spill_dir=None):
    '''构造流式full outer join。

    参数
    ----
    left_rows, right_rows: 只能遍历一次的行迭代器（dict行，含source_row_id）。
    columns: 键列名到'integer'/'string'的映射，类型精确匹配。
    budget_bytes: 逻辑内存预算，编码行字节+record_overhead及其他账均受限。
    open_files: 组件自身spill文件描述符硬上限（至少4）。
    record_overhead: 每行固定记录开销字节。
    meter: 可注入的MemoryMeter兼容对象（charge/release/used/peak/budget）。
    spill_dir: 临时资料根目录；缺省用系统临时目录，资料均在独立子目录内。

    返回JoinStream：迭代产生JoinedRow，关闭或回收时清理全部临时资料。
    '''
    if not isinstance(columns, dict) or not columns:
        raise ValueError('连接键列配置不能为空')
    if meter is None:
        if budget_bytes is None:
            raise ValueError('必须提供budget_bytes或可注入的meter')
        meter = MemoryMeter(budget_bytes)
    elif budget_bytes is not None and getattr(meter, 'budget', None) != budget_bytes:
        raise ValueError('注入计量器预算与budget_bytes不一致')
    stats = JoinStats()
    limiter = FileLimiter(open_files, stats)
    tmpdir = tempfile.mkdtemp(prefix='join-spill-', dir=spill_dir)
    return JoinStream(left_rows, right_rows, columns, record_overhead,
                      meter, limiter, stats, tmpdir)


class JoinStream:
    '''迭代器形态的连接结果；同时支持上下文管理与显式close。'''

    def __init__(self, left_rows, right_rows, columns, overhead, meter,
                 limiter, stats, tmpdir):
        self._left_rows = left_rows
        self._right_rows = right_rows
        self._columns = columns
        self._overhead = overhead
        self.meter = meter
        self.limiter = limiter
        self.stats = stats
        self._tmpdir = tmpdir
        self._iterator = None
        self._closed = False

    def __iter__(self):
        if self._iterator is not None:
            raise RuntimeError('连接结果只能迭代一次')
        # 返回底层生成器而非self：调用方对返回值再做iter()/next()/list()
        # 都作用于生成器，不会重新触发本对象的单次迭代检查。
        self._iterator = self._run()
        return self._iterator

    def __next__(self):
        if self._iterator is None:
            iter(self)
        return next(self._iterator)

    def _run(self):
        meter = self.meter
        stats = self.stats
        limiter = self.limiter
        tmpdir = self._tmpdir
        left_run = right_run = None
        left_null_path = right_null_path = None
        live_groups = []
        auditor = _IdAuditor(meter, limiter, stats, tmpdir)
        try:
            try:
                left_run, left_null_path = _ingest(
                    self._left_rows, 'L', self._columns, self._overhead,
                    meter, limiter, stats, tmpdir, auditor)
                right_run, right_null_path = _ingest(
                    self._right_rows, 'R', self._columns, self._overhead,
                    meter, limiter, stats, tmpdir, auditor)
                # 两侧摄入完成后统一外部归并审计ID唯一性，重复在此显式报错。
                auditor.audit()
            except BaseException:
                # 摄入或审计中途失败：已冻结的run与审计缓冲先释放驻留账。
                for run in (left_run, right_run):
                    if run is not None:
                        run.close()
                auditor.cleanup()
                raise

            left_peek = _Peek(left_run)
            right_peek = _Peek(right_run)
            try:
                while True:
                    left_head = left_peek.head
                    right_head = right_peek.head
                    if left_head is None and right_head is None:
                        break
                    if right_head is None or (left_head is not None
                                              and left_head.key < right_head.key):
                        # 仅左侧有键：整组都是左未匹配（组可能超预算转spill）。
                        key = left_head.key
                        group = _GroupBuffer(key, meter, limiter, stats,
                                             tmpdir, 'L')
                        live_groups.append(group)
                        while (left_peek.head is not None
                               and left_peek.head.key == key):
                            group.take_from(left_peek)
                        group.finish_spill()
                        for item in group.transient():
                            yield _emit_unmatched(key, item, 'L', stats)
                        self._cleanup_groups(live_groups)
                        continue
                    if left_head is None or (right_head is not None
                                             and right_head.key < left_head.key):
                        key = right_head.key
                        group = _GroupBuffer(key, meter, limiter, stats,
                                             tmpdir, 'R')
                        live_groups.append(group)
                        while (right_peek.head is not None
                               and right_peek.head.key == key):
                            group.take_from(right_peek)
                        group.finish_spill()
                        for item in group.transient():
                            yield _emit_unmatched(key, item, 'R', stats)
                        self._cleanup_groups(live_groups)
                        continue
                    # 键相等：拉取两侧同键组，输出完整笛卡尔积。
                    key = left_head.key
                    left_group = _GroupBuffer(key, meter, limiter, stats,
                                              tmpdir, 'L')
                    right_group = _GroupBuffer(key, meter, limiter, stats,
                                               tmpdir, 'R')
                    left_group.peer = right_group
                    right_group.peer = left_group
                    live_groups.extend((left_group, right_group))
                    while left_peek.head is not None and left_peek.head.key == key:
                        left_group.take_from(left_peek)
                    while right_peek.head is not None and right_peek.head.key == key:
                        right_group.take_from(right_peek)
                    left_group.finish_spill()
                    right_group.finish_spill()
                    yield from _block_nested_loops(
                        key, left_group, right_group, meter, limiter, stats)
                    self._cleanup_groups(live_groups)
            finally:
                left_peek.close()
                right_peek.close()

            # null键不参与匹配（含两侧都为null）：按来源L先R后、再按行号输出。
            for side, null_path in (('L', left_null_path),
                                    ('R', right_null_path)):
                if null_path is None:
                    continue
                with _open_spill(null_path, limiter, stats) as reader:
                    for record in reader.records():
                        meter.charge(record.cost)
                        try:
                            yield _emit_unmatched(None, record, side, stats)
                        finally:
                            meter.release(record.cost)
        finally:
            self._cleanup_groups(live_groups)
            for run in (left_run, right_run):
                if run is not None:
                    run.close()
            auditor.cleanup()
            self._cleanup_tmpdir()
            self._closed = True

    @staticmethod
    def _cleanup_groups(groups):
        while groups:
            groups.pop().cleanup()

    def _cleanup_tmpdir(self):
        if not self._tmpdir or not os.path.isdir(self._tmpdir):
            return
        for name in os.listdir(self._tmpdir):
            path = os.path.join(self._tmpdir, name)
            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.unlink(path)
            except OSError:
                pass
        try:
            os.rmdir(self._tmpdir)
        except os.error:
            pass

    def close(self):
        '''提前关闭：关闭生成器触发finally，停止后续输入消费并清理。'''
        if self._closed:
            return
        if self._iterator is not None:
            self._iterator.close()
        else:
            # 从未迭代：没有临时段需要关闭，直接清目录。
            self._cleanup_tmpdir()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
