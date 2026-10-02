from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from .ring import (
    HashRing,
    MigrationSummary,
    Node,
    Placement,
    compare_placements,
)


STATE_FORMAT = "local-consistent-hash-simulator/v1"


@dataclass(frozen=True, slots=True)
class HistoryEvent:
    seq: int
    action: str
    details: dict[str, object]


def migration_to_dict(summary: MigrationSummary) -> dict[str, object]:
    return {
        "common_key_count": summary.common_key_count,
        "changed_key_count": summary.changed_key_count,
        "moved_replica_count": summary.moved_replica_count,
        "total_replica_count": summary.total_replica_count,
        "primary_changed_count": summary.primary_changed_count,
        "changed_key_ratio": summary.changed_key_ratio,
        "moved_replica_ratio": summary.moved_replica_ratio,
        "primary_changed_ratio": summary.primary_changed_ratio,
        "keys": [
            {
                "key": item.key,
                "old_nodes": list(item.old_nodes),
                "new_nodes": list(item.new_nodes),
                "added_nodes": list(item.added_nodes),
                "removed_nodes": list(item.removed_nodes),
                "moved_replicas": item.moved_replicas,
                "primary_changed": item.primary_changed,
            }
            for item in summary.keys
        ],
    }


def _placement_to_dict(placement: Placement) -> dict[str, object]:
    return {
        "key": placement.key,
        "replicas": [
            {
                "rank": replica.rank,
                "node_id": replica.node_id,
                "virtual_node_index": replica.virtual_node_index,
                "token": str(replica.token),
                "fault_domain": replica.fault_domain,
            }
            for replica in placement.replicas
        ],
        "degraded": placement.degraded,
        "warnings": list(placement.warnings),
    }


class ShardSimulator:
    """In-memory simulator with optional deterministic local JSON snapshots."""

    def __init__(
        self,
        nodes: Iterable[Node] = (),
        *,
        virtual_nodes: int = 160,
        replication_factor: int = 1,
    ) -> None:
        self._ring = HashRing(
            nodes,
            virtual_nodes=virtual_nodes,
            replication_factor=replication_factor,
        )
        self._placements: dict[str, Placement] = {}
        self._history: list[HistoryEvent] = []

    @property
    def virtual_nodes(self) -> int:
        return self._ring.virtual_nodes_per_weight

    @property
    def replication_factor(self) -> int:
        return self._ring.replication_factor

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._placements))

    def nodes(self) -> tuple[Node, ...]:
        return self._ring.nodes()

    def ring(self) -> HashRing:
        return self._ring

    def placements(self) -> dict[str, Placement]:
        return dict(self._placements)

    def placement(self, key: str) -> Placement:
        return self._placements[key]

    def history(self) -> tuple[HistoryEvent, ...]:
        return tuple(self._history)

    def add_node(self, node: Node) -> MigrationSummary:
        old_placements = self.placements()
        self._ring.add_node(node)
        return self._rebuild_placements(
            "add_node",
            old_placements,
            {"node_id": node.id, "weight": node.weight, "fault_domain": node.fault_domain},
        )

    def remove_node(self, node_id: str) -> MigrationSummary:
        old_placements = self.placements()
        removed = self._ring.get_node(node_id)
        self._ring.remove_node(node_id)
        return self._rebuild_placements(
            "remove_node",
            old_placements,
            {
                "node_id": node_id,
                "weight": removed.weight,
                "fault_domain": removed.fault_domain,
            },
        )

    def replace_nodes(self, nodes: Sequence[Node]) -> MigrationSummary:
        old_placements = self.placements()
        self._ring = HashRing(
            nodes,
            virtual_nodes=self.virtual_nodes,
            replication_factor=self.replication_factor,
        )
        return self._rebuild_placements(
            "replace_nodes",
            old_placements,
            {"node_ids": [node.id for node in self.nodes()]},
        )

    def place_key(self, key: str) -> Placement:
        placement = self._ring.place_key(key)
        self._placements[key] = placement
        self._append_history(
            "place_key",
            {
                "key": key,
                "nodes": list(placement.node_ids),
                "degraded": placement.degraded,
                "warnings": list(placement.warnings),
            },
        )
        return placement

    def place_keys(self, keys: Iterable[str]) -> dict[str, Placement]:
        return {key: self.place_key(key) for key in keys}

    def rebalance(self) -> MigrationSummary:
        old_placements = self.placements()
        return self._rebuild_placements("rebalance", old_placements, {"keys": self.keys})

    def to_dict(self) -> dict[str, object]:
        return {
            "format": STATE_FORMAT,
            "config": {
                "virtual_nodes": self.virtual_nodes,
                "replication_factor": self.replication_factor,
            },
            "nodes": [
                {"id": node.id, "weight": node.weight, "fault_domain": node.fault_domain}
                for node in self.nodes()
            ],
            "placements": [
                _placement_to_dict(self._placements[key]) for key in self.keys
            ],
            "history": [
                {"seq": event.seq, "action": event.action, "details": event.details}
                for event in self._history
            ],
        }

    @classmethod
    def from_dict(cls, state: dict[str, object]) -> "ShardSimulator":
        if state.get("format") != STATE_FORMAT:
            raise ValueError("unsupported state format")
        config = state.get("config")
        if not isinstance(config, dict):
            raise ValueError("state config must be an object")

        simulator = cls(
            _nodes_from_state(state),
            virtual_nodes=_positive_int(config, "virtual_nodes"),
            replication_factor=_positive_int(config, "replication_factor"),
        )

        placements = state.get("placements")
        if not isinstance(placements, list):
            raise ValueError("state placements must be a list")
        keys = [_key_from_record(record) for record in placements]
        simulator.place_keys(keys)

        history = state.get("history")
        if not isinstance(history, list):
            raise ValueError("state history must be a list")
        simulator._history = [_history_from_record(record) for record in history]
        return simulator

    def save_state(self, path: str | os.PathLike[str]) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.to_dict(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"

        fd: int | None = None
        temporary_name: str | None = None
        try:
            fd, temporary_name = tempfile.mkstemp(
                prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
            )
            with os.fdopen(fd, "w", encoding="utf-8") as temporary_file:
                fd = None
                temporary_file.write(payload)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_name, target)
        finally:
            if fd is not None:
                os.close(fd)
            if temporary_name is not None and os.path.exists(temporary_name):
                os.unlink(temporary_name)

    @classmethod
    def load_state(cls, path: str | os.PathLike[str]) -> "ShardSimulator":
        with Path(path).open("r", encoding="utf-8") as state_file:
            return cls.from_dict(json.load(state_file))

    def _rebuild_placements(
        self,
        action: str,
        old_placements: dict[str, Placement],
        details: dict[str, object],
    ) -> MigrationSummary:
        self._placements = {
            key: self._ring.place_key(key) for key in sorted(old_placements)
        }
        summary = compare_placements(old_placements, self._placements)
        event_details = dict(details)
        event_details["migration"] = migration_to_dict(summary)
        self._append_history(action, event_details)
        return summary

    def _append_history(self, action: str, details: dict[str, object]) -> None:
        self._history.append(
            HistoryEvent(seq=len(self._history) + 1, action=action, details=details)
        )


def _nodes_from_state(state: dict[str, object]) -> list[Node]:
    records = state.get("nodes")
    if not isinstance(records, list):
        raise ValueError("state nodes must be a list")
    nodes: list[Node] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("each state node must be an object")
        node_id = record.get("id")
        fault_domain = record.get("fault_domain", "")
        if not isinstance(node_id, str) or not isinstance(fault_domain, str):
            raise ValueError("node id and fault_domain must be strings")
        nodes.append(
            Node(
                id=node_id,
                weight=_positive_int(record, "weight"),
                fault_domain=fault_domain,
            )
        )
    return nodes


def _positive_int(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def _key_from_record(record: object) -> str:
    if not isinstance(record, dict) or not isinstance(record.get("key"), str):
        raise ValueError("each placement record must contain a string key")
    return record["key"]


def _history_from_record(record: object) -> HistoryEvent:
    if not isinstance(record, dict):
        raise ValueError("each history record must be an object")
    seq = record.get("seq")
    action = record.get("action")
    details = record.get("details")
    if not isinstance(seq, int) or not isinstance(action, str) or not isinstance(details, dict):
        raise ValueError("invalid history record")
    return HistoryEvent(seq=seq, action=action, details=dict(details))
