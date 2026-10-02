from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .ring import Node
from .simulator import ShardSimulator, migration_to_dict


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def _parse_node_spec(spec: str) -> Node:
    parts = spec.split(":", 2)
    node_id = parts[0].strip()
    weight = int(parts[1]) if len(parts) > 1 and parts[1] else 1
    fault_domain = parts[2] if len(parts) > 2 else ""
    return Node(id=node_id, weight=weight, fault_domain=fault_domain)


def _load(path: Path) -> ShardSimulator:
    return ShardSimulator.load_state(path)


def _save(simulator: ShardSimulator, path: Path) -> None:
    simulator.save_state(path)


def _migration_payload(summary: object) -> dict[str, object]:
    return migration_to_dict(summary)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m shard_sim",
        description="Local consistent hashing shard and replica placement simulator",
    )
    parser.add_argument("--state", type=Path, default=Path("shard-state.json"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="create a local state file")
    init.add_argument("--node", action="append", default=[], metavar="ID[:WEIGHT[:DOMAIN]]")
    init.add_argument("--virtual-nodes", type=int, default=160)
    init.add_argument("--replication-factor", type=int, default=1)
    init.add_argument("--force", action="store_true", help="overwrite an existing state file")

    add_node = subparsers.add_parser("add-node", help="add one storage node")
    add_node.add_argument("node_id")
    add_node.add_argument("--weight", type=int, default=1)
    add_node.add_argument("--fault-domain", default="")

    remove_node = subparsers.add_parser("remove-node", help="remove one storage node")
    remove_node.add_argument("node_id")

    place = subparsers.add_parser("place", help="place one or more keys")
    place.add_argument("keys", nargs=argparse.ONE_OR_MORE)

    subparsers.add_parser("rebalance", help="rebuild placement for all tracked keys")
    subparsers.add_parser("status", help="print deterministic JSON state")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "init":
        if args.state.exists() and not args.force:
            raise SystemExit(f"state file already exists: {args.state}")
        simulator = ShardSimulator(
            (_parse_node_spec(spec) for spec in args.node),
            virtual_nodes=args.virtual_nodes,
            replication_factor=args.replication_factor,
        )
        _save(simulator, args.state)
        _print_json({"state": str(args.state), "nodes": len(simulator.nodes())})
        return 0

    if not args.state.exists():
        raise SystemExit(f"state file not found: {args.state}")
    simulator = _load(args.state)

    if args.command == "add-node":
        summary = simulator.add_node(
            Node(id=args.node_id, weight=args.weight, fault_domain=args.fault_domain)
        )
        _save(simulator, args.state)
        _print_json(_migration_payload(summary))
    elif args.command == "remove-node":
        summary = simulator.remove_node(args.node_id)
        _save(simulator, args.state)
        _print_json(_migration_payload(summary))
    elif args.command == "place":
        placements = simulator.place_keys(args.keys)
        _save(simulator, args.state)
        _print_json(
            {
                key: {
                    "nodes": list(placement.node_ids),
                    "degraded": placement.degraded,
                    "warnings": list(placement.warnings),
                }
                for key, placement in placements.items()
            }
        )
    elif args.command == "rebalance":
        summary = simulator.rebalance()
        _save(simulator, args.state)
        _print_json(_migration_payload(summary))
    elif args.command == "status":
        _print_json(simulator.to_dict())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
