import json
from .io import canonical

class Once:
    def __init__(self,rows):self.rows=iter(rows);self.used=False
    def __iter__(self):
        if self.used:raise RuntimeError('输入不可二次遍历')
        self.used=True;return self.rows

def stream_jsonl(path):
    with open(path,encoding='utf-8') as source:
        for number,line in enumerate(source,1):
            if not line.strip():continue
            value=json.loads(line)
            if not isinstance(value,dict):raise ValueError('行不是对象：'+str(number))
            yield value

def typed_key(row,columns):
    result=[]
    for name,kind in columns.items():
        value=row[name]
        if value is None:result.append(None);continue
        expected={'integer':int,'string':str}.get(kind)
        if expected is None or type(value) is not expected:raise ValueError('键类型不符')
        result.append(value)
    return tuple(result)

def row_bytes(row):return len(canonical(row))+64

def synthetic(size,side):
    for i in range(size):yield {'source_row_id':side+f'{i:09}','store':'S1','task':i%3,'value':'合成'+str(i)}
