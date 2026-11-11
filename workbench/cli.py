import argparse
import json
import sys
from .io import inspect_bundle
from .catalog import Catalog

def main():
    parser=argparse.ArgumentParser(description='业务资料读取与本地存档')
    subs=parser.add_subparsers(dest='command',required=True)
    p=subs.add_parser('inspect');p.add_argument('directory')
    p=subs.add_parser('ingest');p.add_argument('directory');p.add_argument('database')
    p=subs.add_parser('catalog');p.add_argument('database')
    args=parser.parse_args()
    try:
        if args.command=='inspect':result=inspect_bundle(args.directory)
        elif args.command=='ingest':result={'import_id':Catalog(args.database).ingest(args.directory)}
        else:result=Catalog(args.database).summary()
        print(json.dumps(result,ensure_ascii=False,indent=2));return 0
    except (ValueError,OSError,KeyError):
        print('资料读取失败，请核对文件与字段约定',file=sys.stderr);return 2
if __name__=='__main__':raise SystemExit(main())
