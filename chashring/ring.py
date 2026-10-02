"""Consistent hash ring with weighted virtual nodes, replication and
fault-domain-aware replica placement.

Determinism guarantees:
  * All hashes use SHA-1 (never Python's salted ``hash()``).
  * The ring is a sorted structure; lookup results depend only on the
    *set* of nodes and their config, never on insertion order.
  * Ties (identical vnode hashes) are broken by node id.
"""

from __future__ import annotations

import bisect
import hashlib
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def _hash(text: str) -> int:
    """Deterministic 64-bit hash of a string via SHA-1."""
    digest = hashlib.sha1(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass(frozen=True)
class Node:
    """A physical storage node.

    weight:        relative capacity; vnode count = base_vnodes * weight.
    fault_domain:  opaque label (rack/zone/...); replicas prefer to spread
                   across distinct fault domains.
    """

    node_id: str
    weight: int = 1
    fault_domain: str = "default"

    def __post_init__(self) -> None:
        if not self.node_id:
            raise ValueError("node_id must be non-empty")
        if self.weight < 1:
            raise ValueError("weight must be >= 1")


@dataclass
class Placement:
    """Result of placing one key."""

    key: str
    nodes: List[str]            # ordered replica node ids (primary first)
    fault_domains: List[str]    # fault domain of each replica
    degraded: bool              # True if replicas could not be spread over
                                # RF distinct fault domains
    distinct_domains: int       # number of distinct fault domains used


class HashRing:
    """Consistent hash ring over a set of weighted nodes."""

    def __init__(self, base_vnodes: int = 100, replication_factor: int = 1):
        if base_vnodes < 1:
            raise ValueError("base_vnodes must be >= 1")
        if replication_factor < 1:
            raise ValueError("replication_factor must be >= 1")
        self.base_vnodes = base_vnodes
        self.replication_factor = replication_factor
        self._nodes: Dict[str, Node] = {}
        # Parallel arrays sorted by vnode hash: _points[i] <-> _owners[i]
        self._points: List[int] = []
        self._owners: List[str] = []
        # Local in-memory history of membership changes.
        self.history: List[dict] = []

    # ------------------------------------------------------------------
    # Membership
    # ------------------------------------------------------------------
    def _vnodes_for(self, node: Node) -> int:
        """Deterministic rule: vnodes scale linearly with weight."""
        return self.base_vnodes * node.weight

    def add_node(self, node: Node) -> None:
        if node.node_id in self._nodes:
            raise ValueError(f"node {node.node_id!r} already exists")
        self._nodes[node.node_id] = node
        for i in range(self._vnodes_for(node)):
            point = _hash(f"{node.node_id}#{i}")
            idx = bisect.bisect_left(self._points, point)
            # Deterministic tie-break on hash collision: order by node id.
            while (
                idx < len(self._points)
                and self._points[idx] == point
                and self._owners[idx] < node.node_id
            ):
                idx += 1
            self._points.insert(idx, point)
            self._owners.insert(idx, node.node_id)
        self._record("add", node.node_id)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            raise KeyError(f"node {node_id!r} does not exist")
        node = self._nodes.pop(node_id)
        kept = [(p, o) for p, o in zip(self._points, self._owners) if o != node_id]
        self._points = [p for p, _ in kept]
        self._owners = [o for _, o in kept]
        self._record("remove", node_id)

    def _record(self, op: str, node_id: str) -> None:
        self.history.append(
            {"op": op, "node": node_id, "members": sorted(self._nodes)}
        )

    @property
    def nodes(self) -> Dict[str, Node]:
        return dict(self._nodes)

    @property
    def vnode_count(self) -> int:
        return len(self._points)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------
    def _walk(self, key: str) -> Iterable[str]:
        """Yield distinct physical node ids in clockwise ring order."""
        if not self._points:
            return
        start = bisect.bisect_left(self._points, _hash(f"key:{key}"))
        seen = set()
        n = len(self._points)
        for offset in range(n):
            owner = self._owners[(start + offset) % n]
            if owner not in seen:
                seen.add(owner)
                yield owner
                if len(seen) == len(self._nodes):
                    break

    def place(self, key: str, replication_factor: Optional[int] = None) -> Placement:
        """Place ``key`` onto ``replication_factor`` distinct physical nodes.

        Replicas prefer distinct fault domains. If the cluster cannot
        provide enough distinct domains, the remaining replicas fall back
        to the next distinct physical nodes in ring order and the result
        is marked ``degraded``.
        """
        rf = replication_factor or self.replication_factor
        if not self._nodes:
            raise RuntimeError("cannot place key: ring is empty")
        if rf > len(self._nodes):
            raise ValueError(
                f"replication_factor {rf} exceeds node count {len(self._nodes)}"
            )

        candidates = list(self._walk(key))
        chosen: List[str] = []
        used_domains = set()
        # Pass 1: prefer nodes whose fault domain is not yet used.
        for nid in candidates:
            if len(chosen) == rf:
                break
            domain = self._nodes[nid].fault_domain
            if domain not in used_domains:
                chosen.append(nid)
                used_domains.add(domain)
        # Pass 2: fill remaining slots (degraded placement).
        if len(chosen) < rf:
            for nid in candidates:
                if len(chosen) == rf:
                    break
                if nid not in chosen:
                    chosen.append(nid)

        domains = [self._nodes[nid].fault_domain for nid in chosen]
        return Placement(
            key=key,
            nodes=chosen,
            fault_domains=domains,
            degraded=len(set(domains)) < rf,
            distinct_domains=len(set(domains)),
        )

    def get_node(self, key: str) -> str:
        """Primary (first replica) node for a key."""
        return self.place(key, 1).nodes[0]

    def map_keys(
        self, keys: Sequence[str], replication_factor: Optional[int] = None
    ) -> Dict[str, List[str]]:
        """Map many keys to their replica node-id lists."""
        return {k: self.place(k, replication_factor).nodes for k in keys}

    # ------------------------------------------------------------------
    # Persistence (local files only)
    # ------------------------------------------------------------------
    def save(self, path: str) -> None:
        """Persist full ring state (config, nodes, history) to a JSON file."""
        state = {
            "base_vnodes": self.base_vnodes,
            "replication_factor": self.replication_factor,
            "nodes": [asdict(self._nodes[nid]) for nid in sorted(self._nodes)],
            "history": self.history,
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)

    @classmethod
    def load(cls, path: str) -> "HashRing":
        """Rebuild a ring from a local state file. Rebuilt ring is
        bit-for-bit identical to the saved one (deterministic rebuild)."""
        with open(path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        ring = cls(
            base_vnodes=state["base_vnodes"],
            replication_factor=state["replication_factor"],
        )
        for nd in state["nodes"]:
            ring.add_node(Node(**nd))
        ring.history = state.get("history", ring.history)
        return ring

    # ------------------------------------------------------------------
    # Distribution diagnostics
    # ------------------------------------------------------------------
    def load_distribution(self, keys: Sequence[str]) -> Dict[str, int]:
        """Count of primary keys per node (load-balance diagnostic)."""
        counts = {nid: 0 for nid in self._nodes}
        for k in keys:
            counts[self.get_node(k)] += 1
        return counts


def compute_migration(
    before: Dict[str, List[str]], after: Dict[str, List[str]]
) -> dict:
    """Compare two key->replicas mappings.

    A key counts as migrated if its replica *set* changed. Returns the
    moved/total counts, the migration ratio and the moved keys.
    """
    common = [k for k in before if k in after]
    moved = [k for k in common if set(before[k]) != set(after[k])]
    primary_moved = [k for k in common if before[k][:1] != after[k][:1]]
    pairs_before = {(k, n) for k in common for n in before[k]}
    pairs_after = {(k, n) for k in common for n in after[k]}
    pairs_moved = len(pairs_before - pairs_after)
    total = len(common)
    return {
        "total_keys": total,
        "moved_keys": len(moved),
        "ratio": (len(moved) / total) if total else 0.0,
        "primary_moved_keys": len(primary_moved),
        "primary_ratio": (len(primary_moved) / total) if total else 0.0,
        "replica_pairs_moved": pairs_moved,
        "replica_pair_ratio": (pairs_moved / len(pairs_before)) if pairs_before else 0.0,
        "moved": moved,
    }
