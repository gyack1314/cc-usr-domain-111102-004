# 有界内存流式 full outer join

`workbench/join.py` 在只能遍历一次的两侧行迭代器上做 full outer join，输出同样是
迭代器，全程不积攒整个输入或结果。它面向机器人遥测与履约订单这类两侧都可能重复
上报同一业务键、且单日高峰数据放不进内存哈希表的场景。

## 连接语义

* 键由 `columns` 配置（列名到 `integer`/`string`）。类型必须**精确**匹配：
  字符串 `'1'` 不等于整数 `1`，布尔值不是整数，字符串按 Unicode 码点比较、
  不做大小写折叠。
* 复合键任一列为 `null` 即不参与匹配，包括左右两边都为 `null`。null 行作为
  未匹配行，按来源（L 先 R 后）、再按输入行号输出。
* 非 null 键的左右重复组输出**完整笛卡尔积**，不去重、不截断。
* 未匹配行以另一侧对象为 `None` 的配对输出，并用 `left_present`/`right_present`
  区分“缺行”与“该行真实存在但某字段为 null”。
* 输出顺序固定：非 null 键升序；键内先按左 `source_row_id`、再按右
  `source_row_id`；最后是 null 未匹配行。
* `source_row_id` 必须为非空字符串且在各自来源唯一；跨来源重复也会被拒绝。

## 内存与临时资料边界

* 每行保留成本 = canonical JSON 编码字节数 + `record_overhead`（默认64）。
  排序缓冲、归并堆首行、重复组缓存、块嵌套循环切块、source_row_id 唯一性审计
  全部计入同一个可注入的 `MemoryMeter` 预算。
* 单行本身超过预算，在任何保留动作之前即抛 `BudgetExceededError`，不会被静默丢弃。
* 排序采用内存段 + spill 段 + 多轮 k 路归并；归并 fan-in 同时受打开文件数和
  **剩余预算**约束，且至少为 2 以保证段数每轮严格收敛；预算连两路首行都放不下时
  显式报错，不以死循环或截断换取结果。
* 单键组大于预算时转 spill 上的块嵌套循环，绝不退回整组 `list`。左表固定为
  外循环，保证键内输出顺序为左 rid、再右 rid。
* source_row_id 唯一性不使用全局内存集合，而是经独立的有界外部审计（spill 后
  按 ID 归并检测相邻重复），输入规模再大也不会因 ID 表撑爆预算。
* `open_files` 是组件自身 spill 描述符的硬上限（调用者的输入迭代器不计入），
  最小值为 4。
* 每个 spill 段的帧为 `uint32 长度 + payload + sha256(payload)`，段尾带魔数、
  记录数与整段摘要。截断、长度异常、校验不符或尾后多余字节都抛
  `SpillCorruptError`，损坏不会被当成正常缺行吞掉。
* 正常耗尽、异常、调用者提前 `close()` 或上下文退出都会清理本次全部临时资料；
  清理对重复关闭幂等。

## 观察口径

`stream.stats`（`JoinStats`）暴露真实运行指标：两侧行数、匹配对数、两侧未匹配数、
spill 段数、创建的临时文件数、spill 读写字节、归并轮次、溢出的大组数、打开文件
描述符峰值。`stream.meter.peak` 是逻辑内存峰值，迭代结束后 `meter.used` 归零。
这些是逻辑记账上界，不声称等于进程 RSS。

## 接口边界

* `full_outer_join(left_rows, right_rows, columns, budget_bytes, ...)` 返回
  `JoinStream`，它是单次迭代器：再次 `iter()` 抛 `RuntimeError`。
* 有界外部排序需要在首个结果产出前完整摄入两侧（单次遍历合同所要求），因此
  提前关闭的保证是停止产出后续结果并立即清理临时资料，而非中断已经开始的摄入。
* 可通过 `meter=` 注入兼容 `charge/release/used/peak/budget` 的自定义计量器；
  注入计量器时 `budget_bytes` 可省略。
* `spill_dir` 可指定临时资料根目录，每次运行使用其下独立子目录。

## 测试

`tests/test_join.py` 用很小预算强制多个排序段、单键超预算的块嵌套循环与多轮
归并，并与 `tests/join_checks.py` 中一次性小数据的朴素袋连接逐项对照；另覆盖
单次遍历输入、提前停止的消费者、损坏段检出、打开文件数与内存峰值约束。
虚构遥测、订单与大组生成器见 `workbench/domain.py`。
