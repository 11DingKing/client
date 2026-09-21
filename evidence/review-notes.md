# project-06 交付复盘记录

## Prompt 分解

P1：已发现的 Source GVR 在读取前消失或 NoMatch 时，命令刷新发现并从新视图重新列举。
P2：任一类型的分页 resourceVersion 过期时，该轮已聚合对象被丢弃，重试结果不重复也不缺页。
P3：单个 Source 类型出现独立临时错误时，其余已确认类型仍可输出，并明确标出遗漏类型和原因。
P4：单个类型权限不足时不伪装成完整成功，命令返回非成功结果且不隐藏已确认对象。
P5：最终对象只对应有效发现视图，并按身份去重后维持确定排序和既有表格字段。
P6：刷新或重列耗尽后返回稳定诊断，不混合失败轮次的对象、GVR 或分页 token。
P7：无法读取 CRD 列表时，现有内置 Source 类型回退行为保持兼容。
P8：类型过滤、空列表、单类型查询和正常 source list 输出格式不回归。

## Prompt-Rubric 对齐表

P1 -> R1：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P2 -> R2：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P3 -> R3：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P4 -> R4：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P5 -> R5：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P6 -> R6：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P7 -> R7：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。
P8 -> R8：原始 Prompt 明示的场景、边界或可观察结果由该 Rubric 直接验收。

双向覆盖结论：P1-P8 均有对应 Rubric，R1-R8 均可回指 Prompt 明示需求。无额外验收要求。

## 生产路径与生命周期审计

生产入口是 kn source list；状态 owner 是单次列表轮的 discovery 视图、分页聚合器和 partial-result 错误，API 边界是 CRD discovery 与 dynamic client List。顺序为发现、逐类分页、整轮提交、去重排序；NoMatch 刷新视图，expired token 丢弃整轮，单类错误保留其他结果并返回非成功。可控交错：第二页返回 expired，已聚合第一页被清空，新 resourceVersion 从首页重列，结果无重复。

## Rubric 结果

```text
1 通过
2 通过
3 通过
4 通过
5 通过
6 通过
7 通过
8 通过
```
