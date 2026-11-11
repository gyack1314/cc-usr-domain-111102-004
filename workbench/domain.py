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

def telemetry_rows(size,dup_every=0):
    '''虚构机器人遥测行流：复合键(store,task)，含字符串键、null与重复上报。'''
    for i in range(size):
        task=None if i%17==0 else i%11
        yield {'source_row_id':f'T{i:08d}','store':f'S{i%7}','task':task,
               'battery':100-i%50,'note':'遥测'+str(i)}
        if dup_every and (i+1)%dup_every==0:
            yield {'source_row_id':f'T{i:08d}D','store':f'S{i%7}',
                   'task':task,'battery':7,'note':'重复遥测'}

def order_rows(size,dup_every=0):
    '''虚构履约订单动作行流：与遥测同键，刻意制造缺行与多余键。'''
    for i in range(size):
        task=None if i%19==0 else i%11
        yield {'source_row_id':f'O{i:08d}','store':f'S{(i+2)%7}','task':task,
               'action':'拣货' if i%2 else '出库','note':'订单'+str(i)}
        if dup_every and (i+1)%dup_every==0:
            yield {'source_row_id':f'O{i:08d}D','store':f'S{(i+2)%7}',
                   'task':task,'action':'重试','note':'重复订单'}

def big_group_rows(side,group_size,key_store='SX',key_task=99,prefix=0):
    '''单键超大重复组生成器：所有非null行同键，用于强制组级spill路径。'''
    for i in range(group_size):
        yield {'source_row_id':f'{side}{prefix+i:08d}','store':key_store,
               'task':key_task,'payload':'块'+('甲'*8)+str(i)}
