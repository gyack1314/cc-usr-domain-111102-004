import argparse
import json
from .domain import synthetic
if __name__=='__main__':
    p=argparse.ArgumentParser(description='输出合成行流');p.add_argument('count',type=int);p.add_argument('--side',default='L');args=p.parse_args()
    if args.count<0:p.error('行数不能为负')
    for row in synthetic(args.count,args.side):print(json.dumps(row,ensure_ascii=False))
