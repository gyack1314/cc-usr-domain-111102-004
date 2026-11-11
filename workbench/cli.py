import argparse
import json
import sys
from .io import inspect_bundle
from .catalog import Catalog
from .domain import stream_jsonl
from .join import full_outer_join


def _parse_columns(text):
    columns = {}
    for item in text.split(','):
        name, kind = item.split(':')
        if kind not in ('integer', 'string'):
            raise ValueError('键类型仅支持integer/string')
        columns[name] = kind
    return columns


def main():
    parser = argparse.ArgumentParser(description='业务资料读取与本地存档')
    subs = parser.add_subparsers(dest='command', required=True)
    p = subs.add_parser('inspect'); p.add_argument('directory')
    p = subs.add_parser('ingest'); p.add_argument('directory'); p.add_argument('database')
    p = subs.add_parser('catalog'); p.add_argument('database')
    p = subs.add_parser('join', description='有界内存流式full outer join')
    p.add_argument('left_jsonl')
    p.add_argument('right_jsonl')
    p.add_argument('--keys', required=True,
                   help='逗号分隔的列:类型，如 store:string,task:integer')
    p.add_argument('--budget-bytes', type=int, required=True)
    p.add_argument('--open-files', type=int, default=8)
    p.add_argument('--record-overhead', type=int, default=64)
    args = parser.parse_args()
    try:
        if args.command == 'inspect':
            result = inspect_bundle(args.directory)
        elif args.command == 'ingest':
            result = {'import_id': Catalog(args.database).ingest(args.directory)}
        elif args.command == 'catalog':
            result = Catalog(args.database).summary()
        else:
            columns = _parse_columns(args.keys)
            stream = full_outer_join(
                stream_jsonl(args.left_jsonl), stream_jsonl(args.right_jsonl),
                columns, budget_bytes=args.budget_bytes,
                open_files=args.open_files,
                record_overhead=args.record_overhead)
            for row in stream:
                print(json.dumps(row.as_dict(), ensure_ascii=False))
            print(json.dumps(stream.stats.as_dict(), ensure_ascii=False),
                  file=sys.stderr)
            return 0
        print(json.dumps(result, ensure_ascii=False, indent=2)); return 0
    except (ValueError, OSError, KeyError):
        print('资料读取失败，请核对文件与字段约定', file=sys.stderr); return 2
if __name__ == '__main__': raise SystemExit(main())
