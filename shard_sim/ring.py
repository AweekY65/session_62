from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from hashlib import sha256
from typing import Iterable, Mapping


DEFAULT_FAULT_DOMAIN = ""


def _sha256(value: str) -> int:
    digest = sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest, "big")


def hash_key(key: str) -> int:
    return _sha256(f"key\x00{key}")


def hash_virtual_node(node_id: str, virtual_index: int, collision_salt: int = 0) -> int:
    suffix = f"\x00{collision_salt}" if collision_salt else ""
    return _sha256(f"virtual-node\x00{node_id}\x00{virtual_index}{suffix}")


@dataclass(frozen=True, slots=True)
class Node:
    id: str
    weight: int = 1
    fault_domain: str = DEFAULT_FAULT_DOMAIN

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("node id must not be empty")
        if not isinstance(self.weight, int) or self.weight <= 0:
            raise ValueError(f"node {self.id!r} weight must be a positive integer")
        if "\x00" in self.id or "\x00" in self.fault_domain:
            raise ValueError("node id and fault domain must not contain NUL characters")


@dataclass(frozen=True, slots=True)
class VirtualNode:
    node_id: str
    index: int
    token: int
    fault_domain: str


@dataclass(frozen=True, slots=True)
class Replica:
    rank: int
    node_id: str
    virtual_node_index: int
    token: int
    fault_domain: str


@dataclass(frozen=True, slots=True)
class Placement:
    key: str
    replicas: tuple[Replica, ...]
    degraded: bool
    warnings: tuple[str, ...]

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(replica.node_id for replica in self.replicas)


@dataclass(frozen=True, slots=True)
class KeyMigration:
    key: str
    old_nodes: tuple[str, ...]
    new_nodes: tuple[str, ...]
    added_nodes: tuple[str, ...]
    removed_nodes: tuple[str, ...]
    moved_replicas: int
    primary_changed: bool


@dataclass(frozen=True, slots=True)
class MigrationSummary:
    common_key_count: int
    changed_key_count: int
    moved_replica_count: int
    total_replica_count: int
    primary_changed_count: int
    changed_key_ratio: float
    moved_replica_ratio: float
    primary_changed_ratio: float
    keys: tuple[KeyMigration, ...]


class HashRing:
    """Deterministic SHA-256 consistent hash ring with weighted virtual nodes."""

    def __init__(
        self,
        nodes: Iterable[Node] = (),
        *,
        virtual_nodes: int = 160,
        replication_factor: int = 1,
    ) -> None:
        if virtual_nodes <= 0:
            raise ValueError("virtual_nodes must be positive")
        if replication_factor <= 0:
            raise ValueError("replication_factor must be positive")

        self._base_virtual_nodes = virtual_nodes
        self._replication_factor = replication_factor
        self._nodes: dict[str, Node] = {}
        for node in nodes:
            self._add_node_without_rebuild(node)
        self._rebuild()

    @property
    def virtual_nodes_per_weight(self) -> int:
        return self._base_virtual_nodes

    @property
    def replication_factor(self) -> int:
        return self._replication_factor

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    def nodes(self) -> tuple[Node, ...]:
        return tuple(self._nodes[node_id] for node_id in sorted(self._nodes))

    def get_node(self, node_id: str) -> Node:
        return self._nodes[node_id]

    def virtual_node_count(self, node_id: str) -> int:
        self.get_node(node_id)
        return self._virtual_node_count(self._nodes[node_id])

    def virtual_nodes(self) -> tuple[VirtualNode, ...]:
        return tuple(self._token_to_vnode[token] for token in self._tokens)

    def add_node(self, node: Node) -> "HashRing":
        self._add_node_without_rebuild(node)
        self._rebuild()
        return self

    def remove_node(self, node_id: str) -> "HashRing":
        try:
            del self._nodes[node_id]
        except KeyError as exc:
            raise KeyError(f"unknown node: {node_id!r}") from exc
        self._rebuild()
        return self

    def place_key(self, key: str, replication_factor: int | None = None) -> Placement:
        if not key:
            raise ValueError("key must not be empty")
        factor = self._replication_factor if replication_factor is None else replication_factor
        if factor <= 0:
            raise ValueError("replication_factor must be positive")

        warnings: list[str] = []
        if not self._tokens:
            return Placement(key=key, replicas=(), degraded=True, warnings=("ring_has_no_nodes",))

        key_token = hash_key(key)
        start = bisect_left(self._tokens, key_token)
        ordered_indexes = [*range(start, len(self._tokens)), *range(0, start)]

        selected: list[Replica] = []
        selected_nodes: set[str] = set()
        selected_domains: set[str] = set()
        first_candidates: dict[str, tuple[Node, VirtualNode, int]] = {}
        first_candidate_order: list[str] = []

        for token_index in ordered_indexes:
            token = self._tokens[token_index]
            vnode = self._token_to_vnode[token]
            if vnode.node_id in selected_nodes:
                continue

            node = self._nodes[vnode.node_id]
            if node.id not in first_candidates:
                first_candidates[node.id] = (node, vnode, token)
                first_candidate_order.append(node.id)

            if node.fault_domain in selected_domains:
                continue

            self._append_replica(selected, node, vnode, token)
            selected_nodes.add(node.id)
            selected_domains.add(node.fault_domain)
            if len(selected) == factor:
                break

        if len(selected) < factor:
            for node_id in first_candidate_order:
                if node_id in selected_nodes:
                    continue
                node, vnode, token = first_candidates[node_id]
                self._append_replica(selected, node, vnode, token)
                selected_nodes.add(node_id)
                selected_domains.add(node.fault_domain)
                if len(selected) == factor:
                    break

        if len(selected) < factor:
            warnings.append("not_enough_physical_nodes")

        domains = [replica.fault_domain for replica in selected]
        if len(set(domains)) < len(domains):
            warnings.append("fault_domain_degraded")

        degraded = bool(warnings)
        return Placement(
            key=key,
            replicas=tuple(selected),
            degraded=degraded,
            warnings=tuple(warnings),
        )

    def place_keys(
        self,
        keys: Iterable[str],
        replication_factor: int | None = None,
    ) -> dict[str, Placement]:
        return {key: self.place_key(key, replication_factor) for key in keys}

    def _virtual_node_count(self, node: Node) -> int:
        return max(1, node.weight * self._base_virtual_nodes)

    def _add_node_without_rebuild(self, node: Node) -> None:
        if node.id in self._nodes:
            raise ValueError(f"duplicate node id: {node.id!r}")
        self._nodes[node.id] = node

    def _rebuild(self) -> None:
        token_to_vnode: dict[int, VirtualNode] = {}
        for node in self.nodes():
            for index in range(self._virtual_node_count(node)):
                salt = 0
                while True:
                    token = hash_virtual_node(node.id, index, salt)
                    if token not in token_to_vnode:
                        token_to_vnode[token] = VirtualNode(
                            node_id=node.id,
                            index=index,
                            token=token,
                            fault_domain=node.fault_domain,
                        )
                        break
                    salt += 1

        self._tokens = tuple(sorted(token_to_vnode))
        self._token_to_vnode = token_to_vnode

    @staticmethod
    def _append_replica(
        selected: list[Replica],
        node: Node,
        vnode: VirtualNode,
        token: int,
    ) -> None:
        selected.append(
            Replica(
                rank=len(selected),
                node_id=node.id,
                virtual_node_index=vnode.index,
                token=token,
                fault_domain=node.fault_domain,
            )
        )


def compare_placements(
    old_placements: Mapping[str, Placement],
    new_placements: Mapping[str, Placement],
) -> MigrationSummary:
    """Count replica ownership changes for keys present in both mappings."""

    key_migrations: list[KeyMigration] = []
    moved_replica_count = 0
    primary_changed_count = 0
    total_replica_count = 0

    for key in sorted(set(old_placements).intersection(new_placements)):
        old = old_placements[key]
        new = new_placements[key]
        old_nodes = set(old.node_ids)
        new_nodes = set(new.node_ids)
        added = tuple(sorted(new_nodes - old_nodes))
        removed = tuple(sorted(old_nodes - new_nodes))
        moved = max(len(added), len(removed))
        primary_changed = bool(
            old.replicas
            and new.replicas
            and old.replicas[0].node_id != new.replicas[0].node_id
        )

        if moved:
            key_migrations.append(
                KeyMigration(
                    key=key,
                    old_nodes=old.node_ids,
                    new_nodes=new.node_ids,
                    added_nodes=added,
                    removed_nodes=removed,
                    moved_replicas=moved,
                    primary_changed=primary_changed,
                )
            )
        moved_replica_count += moved
        primary_changed_count += int(primary_changed)
        total_replica_count += len(new.replicas)

    common_key_count = len(set(old_placements).intersection(new_placements))
    changed_key_count = len(key_migrations)
    return MigrationSummary(
        common_key_count=common_key_count,
        changed_key_count=changed_key_count,
        moved_replica_count=moved_replica_count,
        total_replica_count=total_replica_count,
        primary_changed_count=primary_changed_count,
        changed_key_ratio=(
            changed_key_count / common_key_count if common_key_count else 0.0
        ),
        moved_replica_ratio=(
            moved_replica_count / total_replica_count if total_replica_count else 0.0
        ),
        primary_changed_ratio=(
            primary_changed_count / common_key_count if common_key_count else 0.0
        ),
        keys=tuple(key_migrations),
    )
