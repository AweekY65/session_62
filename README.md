# 本地一致性哈希分片与副本放置模拟器

纯 Python 标准库实现（Python 3.8+，零第三方依赖）。所有节点信息、虚拟节点、
分片映射、历史状态与测试数据只保存在**内存或本地 JSON 文件**中，不依赖
etcd、Consul、数据库、云服务或任何外部服务。

## 目录结构

- `chashring/ring.py` — 核心库：`HashRing` / `Node` / `Placement` / `compute_migration`
- `main.py` — CLI 模拟器（扩缩容、迁移统计、故障域降级、持久化演示）
- `test_consistent_hash.py` — 28 个自动化测试（unittest，终端直接运行）

## 哈希环

- 使用 **SHA-1** 的前 8 字节作为 64 位哈希（不使用 Python 内置 `hash()`，
  避免随机盐导致跨进程不确定）。
- 环结构为按哈希值排序的平行数组 `_points` / `_owners`，查找用二分
  （`bisect`）。哈希冲突时按 `node_id` 字典序打破平局，保证确定性。
- key 的哈希为 `SHA1("key:" + key)`，顺时针找到的第一个虚拟节点即主副本。

## 虚拟节点与权重

- 每个物理节点的虚拟节点数 = `base_vnodes × weight`（确定规则：与权重线性
  成正比）。权重为 3 的节点获得的虚拟节点数与期望负载均为权重 1 节点的 3 倍。
- 虚拟节点哈希为 `SHA1("{node_id}#{i}")`，`i ∈ [0, vnodes)`。
- 虚拟节点越多负载越均衡（测试 `test_more_vnodes_improve_balance` 验证）。

## 副本策略与故障域

- `replication_factor`（RF）个副本必须落在 **RF 个不同物理节点**上：从 key
  位置顺时针遍历环，跳过已选物理节点。
- 故障域感知放置分两轮（确定性贪心）：
  1. 按环序优先选择故障域尚未使用的节点；
  2. 故障域数量不足时，按环序补齐剩余副本，并将结果标记为
     `Placement.degraded = True`（明确报告降级，`distinct_domains` 给出实际
     分散到的域数）。
- `RF > 节点数` 时拒绝放置（`ValueError`）。

## 确定性

相同配置与节点集合，无论节点加入顺序如何，映射结果完全一致：环是有序结构，
只取决于节点集合；测试通过正序/逆序/随机洗牌三种加入顺序验证。

## 迁移统计

`compute_migration(before, after)` 对比两次 `map_keys` 结果，输出：

- `ratio` — 副本集合发生变化的 key 比例；
- `primary_ratio` — 主副本迁移的 key 比例（一致性哈希理论下界为 1/N）；
- `replica_pair_ratio` — (key, node) 副本对变化比例。

扩缩容时只有受影响的 key 迁移（测试中验证迁移的 key 全部迁往新节点）。

## 复杂度

| 操作 | 复杂度 |
|---|---|
| 添加节点 | O(W·B · log R)，W=权重，B=base_vnodes，R=环上虚拟节点总数 |
| 删除节点 | O(R) |
| 单 key 放置 | O(log R + 环上步行)，实际只需走到集齐 RF 个节点/域 |
| N 个 key 映射 | O(N · (log R + RF)) |
| 空间 | O(R) |

## 运行

```bash
# 完整模拟演示（扩缩容 + 迁移比例 + 故障域降级 + 持久化）
python3 main.py demo

# 查询指定 key 的放置
python3 main.py place user:1 user:2

# 保存 / 加载环状态（本地 JSON 文件）
python3 main.py save ring_state.json
python3 main.py load ring_state.json

# 运行全部测试（终端直接输出，无需 GUI）
python3 -m unittest test_consistent_hash -v
```

测试覆盖：节点扩缩容、权重与虚拟节点数、副本不重复落点、故障域分散与降级
报告、输入顺序无关的确定性、单节点/空环/200 节点等极端规模、迁移比例上界、
状态持久化往返一致性与历史记录。
