# 本地原始资料目录

Catalog通过SQLite事务保存整份资料包，不改写已入库记录。相同包摘要再次导入返回原import_id；文件名、文件次序和内容均参与包身份。source_rows按来源文件与零起始ordinal存储原始JSON，字段类型保持不变。它不推断任何领域关系，也不生成业务结果。

`python -m workbench.cli ingest samples state/raw.sqlite3` 导入合成资料，`python -m workbench.cli catalog state/raw.sqlite3` 查看已有包。数据库只保存调用者选择的原始资料，state目录不进入版本控制。
