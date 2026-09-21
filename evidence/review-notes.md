# project-04 交付复盘记录

## Prompt 分解

P1：每次 resourceVersion 冲突重试都从最新 Subscription 重新构造更新，而不复用上一轮对象。
P2：本次明确修改的 subscriber、reply 和 dead-letter sink 在同一重试轮次重新解析并一起提交。
P3：未在命令中指定的 Subscription 字段保留冲突后最新的服务端值，不被旧快照覆盖。
P4：任一目标解析失败时不提交另外两个已解析引用，并返回可定位到对应目标的错误。
P5：冲突耗尽或权限失败不会留下半份目标组合，下一次命令可从服务端当前状态继续。
P6：命令打印成功前会确认服务端保存的三个 Destination 与本次修改意图一致。
P7：没有冲突时，Subscription create/update 及 URI、可寻址资源两类 sink 解析保持兼容。
P8：只修改一个 sink 时，其余目标和现有成功、错误输出格式保持不变。

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

生产入口是 Subscription update 命令；状态 owner 是 Kubernetes Subscription，API 边界是目标解析器与 Kubernetes Update/Get。每轮冲突重试先 Get 最新对象，再解析本次显式 sink，一次 Update 提交，最后 Get 回读三个 Destination；解析失败在 Update 前返回，无外部清理对象。可控交错：第一次 Update 冲突后服务端同时改变未指定字段并重建 reply sink，重试轮重新解析 reply、保留最新未指定字段并成功回读。

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
