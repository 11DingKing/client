# project-05 交付复盘记录

## Prompt 分解

P1：Kubernetes 更新发生 resourceVersion 冲突时，apply 重新读取并合并目标声明，不覆盖并发服务端变更。
P2：Kubernetes 路径只在服务端回读与目标一致后报告已应用，并准确区分 changed 与 unchanged。
P3：请求等待 Ready 时，超时或终止会返回未完成结果，不把已提交但未就绪的 Service 报告为完成。
P4：GitOps 路径先完整序列化并校验临时文件，再原子替换正式 YAML 或 JSON。
P5：GitOps 序列化、写入或发布失败时，上一份正式文件保持完整且临时产物可被后续重试清理。
P6：任一路径重试都会读取实际集群对象或磁盘内容重新计算，不复用失败轮次的注解、changed 或输出。
P7：create、update、unchanged 和无需等待的 service apply 行为保持兼容。
P8：现有 last-applied 配置、YAML/JSON 格式和正常成功输出保持兼容。

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

生产入口是 kn service apply；状态 owner 是 Kubernetes Service 或 GitOps 正式文件，边界是 Kubernetes Patch/Get/Ready watch 与本地 rename。顺序为读取、合并、提交、回读、可选等待；GitOps 为临时写入、校验、原子改名。失败清理临时文件并保留旧正式文件。可控交错：提交仅修改 annotation 的合法声明，PATCH 成功而 Generation 不变，函数因 ObservedGeneration 相等返回 unchanged。

## Rubric 结果

```text
1 通过
2 未通过 Kubernetes PATCH 成功后用 saved.Generation 与 saved.Status.ObservedGeneration 判断 changed，仅元数据声明变更已提交却被报告 unchanged
3 通过
4 通过
5 通过
6 通过
7 通过
8 通过
```
