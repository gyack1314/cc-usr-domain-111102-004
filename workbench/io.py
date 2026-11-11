import hashlib
import json
import os
import tempfile
from pathlib import Path

def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')

def digest(value):return hashlib.sha256(canonical(value)).hexdigest()

def read_json(path):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('JSON对象含重复字段')
            result[key]=value
        return result
    def bad(value):raise ValueError('不允许非有限数值')
    return json.loads(Path(path).read_text(encoding='utf-8'),object_pairs_hook=pairs,parse_constant=bad)

def write_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    data=canonical(value)+b'\n';fd,name=tempfile.mkstemp(dir=path.parent,prefix='.writing-')
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(data);stream.flush();os.fsync(stream.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)

def read_bundle(root):
    root=Path(root).resolve();manifest=read_json(root/'bundle.json');tables={}
    for item in manifest['files']:
        raw=Path(item['path']);path=(root/raw).resolve()
        if raw.is_absolute() or not path.is_relative_to(root):raise ValueError('资料路径越界')
        if item['path'] in tables:raise ValueError('资料路径重复')
        rows=read_json(path)
        if not isinstance(rows,list) or any(not isinstance(r,dict) for r in rows):raise ValueError('资料应为对象数组')
        tables[item['path']]=rows
    return manifest,tables

def inspect_bundle(root):
    manifest,tables=read_bundle(root)
    return {'name':manifest['name'],'files':[{'path':k,'rows':len(v),'sha256':digest(v)} for k,v in tables.items()]}
