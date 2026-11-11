# 遥测与履约明细的有界内存外连接资料库

只读单次JSONL行流、连接键类型检查、有界内存流式full outer join与可控大组资料生成器。 资料在samples，数据语义见docs/domain.md，本地存档说明见docs/catalog.md，外连接合同见docs/join.md。

## 运行

需要Python 3.11以上，仅使用标准库。

```bash
python -m workbench.cli inspect samples
python -m workbench.cli ingest samples state/raw.sqlite3
python -m workbench.cli catalog state/raw.sqlite3
python -m workbench.cli join samples/left.jsonl samples/right.jsonl \
  --keys store:string,task:integer --budget-bytes 2048 --open-files 6
python -m unittest discover -s tests -v
python -m compileall -q workbench tests
```

inspect查看文件行数和摘要；ingest将原始包放入SQLite；catalog列出原始包。join逐行输出full outer join结果（每行含left/right原始对象与left_present/right_present标志），统计（spill段数、读写字节、归并轮次、峰值文件描述符等）打印到stderr。测试覆盖已有资料读取、存档幂等、领域原语与外连接的朴素袋连接逐项对照，compileall检查源码编译。业务计算不会由存档命令自动执行。

## 容器

```bash
docker build -t local-workbench .
docker run --rm --network none local-workbench
docker run --rm --network none local-workbench python -m unittest discover -s tests -v
```

程序不需要外部账号或常驻服务，容器默认查看合成资料。state与output为本地工作目录，不进入版本控制。workbench/io.py负责JSON与文件边界，catalog.py负责原始资料事务，domain.py只提供上述已有能力。

`python -m workbench.generate 10000 --side L` 以JSONL逐行输出重复键合成资料，不进行连接。
