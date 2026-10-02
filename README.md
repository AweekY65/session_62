# 本地一致性哈希分片与副本放置模拟器

这是一个零外部依赖的本地模拟器，使用 Python 标准库实现。节点、虚拟节点、key 到分片的映射、副本位置、迁移统计、历史事件和持久化状态都只存在于进程内存或本地 JSON 文件中；不使用 etcd、Consul、数据库、云服务或任何网络服务。

## 运行要求

- Python 3.10+
- 无需安装第三方依赖

## 快速开始

```bash
./run_tests.sh
```

也可以直接使用标准库 `unittest`：

```bash
python3 -m unittest discover -s tests -v
```

命令行示例：

```bash
python3 -m shard_sim --state demo-state.json init \
  --virtual-nodes 160 \
  --replication-factor 3 \
  --node node-a:1:rack-a \
  --node node-b:2:rack-b \
  --node node-c:1:rack-c

python3 -m shard_sim --state demo-state.json place user:1 user:2
python3 -m shard_sim --state demo-state.json add-node node-d --weight 2 --fault-domain rack-d
python3 -m shard_sim --state demo-state.json status
```

节点参数格式为 `ID[:权重[:故障域标签]]`。例如：

- `node-a`：权重 1，故障域为空，归入 `default` 域。
- `node-b:3`：权重 3，故障域为空。
- `node-c:2:rack-c`：权重 2，故障域为 `rack-c`。

## 哈希环

实现在 `shard_sim/ring.py`：

- 使用 SHA-256 将 key 和虚拟节点映射到同一个 256 位整数环。
- key hash 使用独立前缀 `key\0`，虚拟节点 hash 使用 `virtual-node\0`，避免不同对象的输入混淆。
- 对 key 从其 hash 位置顺时针扫描，遇到的第一个可用物理节点负责主副本。
- SHA-256 token 冲突时使用确定性 salt 重新生成，直到 token 唯一。
- 环按 token 排序，构建前节点按节点 ID 排序；因此相同配置和节点集合与输入顺序无关。

## 虚拟节点与权重

每个物理节点的虚拟节点数量按固定规则计算：

```text
virtual_node_count(node) = node.weight * configured_virtual_nodes
```

权重必须是正整数。权重越大，节点在环上拥有的虚拟 token 越多，获得的 key 期望比例越高。默认值为：

- `virtual_nodes=160`
- `weight=1`
- `replication_factor=1`

增加虚拟节点数量可以降低随机负载的方差，但会增加环内存和一次扫描所需的常数时间。

## 副本策略

副本选择从 key hash 的顺时针位置开始遍历虚拟节点，并遵守以下规则：

1. 同一 key 的副本不能落在同一个物理节点上，即使该节点拥有多个虚拟节点。
2. 第一轮优先选择尚未使用的故障域。
3. 如果不同故障域数量不足，第二轮才复用故障域，并在结果中返回 `fault_domain_degraded`。
4. 如果物理节点数少于副本数，返回所有可用节点，并在结果中返回 `not_enough_physical_nodes`。
5. 未显式标注故障域的节点统一视为 `default` 域；多个未标注节点会触发故障域降级，因为模拟器无法证明它们彼此隔离。

`Placement` 对象包含按副本顺序排列的 `replicas`、`degraded` 和 `warnings`。调用方可以明确区分“成功放置但无法满足故障域约束”和完全正常的结果。

## 动态扩缩容与迁移

一致性哈希的目标是让拓扑变化只影响环上相邻所有权段。副本因子为 1 时，从 `N` 个等权重节点扩展到 `N+1` 个节点，期望只有约 `1/(N+1)` 的 key 改变主节点。

`compare_placements()` 和 `ShardSimulator.add_node()` / `remove_node()` 会统计：

- `common_key_count`：新旧映射中都存在的 key 数。
- `changed_key_count`：至少一个副本节点集合发生变化的 key 数。
- `moved_replica_count`：副本迁移工作量，按 `max(新增副本数, 删除副本数)` 计算。
- `total_replica_count`：新映射的总副本数。
- `primary_changed_count`：主副本节点发生变化的 key 数。
- `changed_key_ratio`：受影响 key / 共同 key。
- `moved_replica_ratio`：迁移副本 / 新总副本。
- `primary_changed_ratio`：主副本变化 key / 共同 key。

统计只比较共同 key，避免把“新录入 key”误算成迁移。每个变化 key 的详细新增、删除和主副本变化也会返回。

## 确定性

以下设计保证相同输入集合产生相同结果：

- SHA-256 是无随机种子的确定性哈希函数。
- 节点构建前按 ID 排序。
- token 按整数大小排序。
- 迁移详情和状态文件中的 key、节点、历史事件都按稳定顺序输出。
- JSON 持久化使用 `sort_keys=True` 和固定缩进，不写入 wall-clock 时间。

## 本地状态

`ShardSimulator` 默认完全在内存中运行。调用 `save_state(path)` 后会把以下内容保存到本地 JSON：

- 配置：虚拟节点基数和副本因子。
- 节点：节点 ID、权重、故障域。
- 已跟踪 key 的当前副本放置。
- 历史事件：录入 key、加入节点、删除节点、替换拓扑、手动 rebalance。

保存采用“同目录临时文件 + `os.replace` 原子替换”。程序不会监听网络，也不会访问任何外部协调服务。

## Python API

```python
from shard_sim import HashRing, Node, ShardSimulator

ring = HashRing(
    [
        Node("node-a", weight=1, fault_domain="rack-a"),
        Node("node-b", weight=2, fault_domain="rack-b"),
        Node("node-c", weight=1, fault_domain="rack-c"),
    ],
    virtual_nodes=160,
    replication_factor=3,
)

placement = ring.place_key("user:42")
print(placement.node_ids, placement.degraded, placement.warnings)

simulator = ShardSimulator(ring.nodes(), virtual_nodes=160, replication_factor=3)
simulator.place_key("user:42")
summary = simulator.add_node(Node("node-d", weight=1, fault_domain="rack-d"))
print(summary.moved_replica_ratio)
simulator.save_state("local-state.json")
```

## 复杂度

设物理节点数为 `N`，每个权重单位的虚拟节点数为 `V`，总虚拟节点数为 `M`，副本因子为 `R`：

- 构建环：`O(M log M)`。
- 环内存：`O(M)`。
- 查找单个 key：使用二分定位 token，复杂度为 `O(log M + S)`；`S` 是找到 `R` 个不同物理节点前扫描的虚拟节点数。
- 添加或删除节点：重建环为 `O(M log M)`；对 `K` 个已跟踪 key 重新放置为 `O(K(log M + S))`。
- 比较两次映射：`O(KR)`。
- 保存状态：`O((N + KR + H) log(...))` 量级的 JSON 序列化与本地写入，`H` 是历史事件大小。

## 测试覆盖

自动化测试位于 `tests/test_ring.py`，覆盖：

- 基础映射与虚拟节点数量。
- 动态添加、删除节点。
- 权重对应的虚拟节点数量和加权负载倾向。
- 副本因子、副本不重复物理节点、节点不足降级。
- 故障域优先分散、故障域复用和明确告警。
- 节点输入顺序不影响结果。
- 单节点和 500 节点等极端节点数量。
- 扩容迁移比例、删除节点只迁移受影响 key。
- 本地 JSON 状态往返和字节级确定性。
- CLI 本地工作流。

运行测试：

```bash
./run_tests.sh
# 或
python3 -m unittest discover -s tests -v
```

测试输出直接显示在终端，不依赖 GUI 或外部服务。
