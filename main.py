#!/usr/bin/env python3
"""Local consistent-hashing sharding & replica placement simulator (CLI).

Everything lives in memory or local files -- no etcd/Consul/DB/cloud.

Usage:
    python3 main.py demo                 # full simulation walkthrough
    python3 main.py place KEY [KEY...]   # place keys on the demo cluster
    python3 main.py save FILE / load FILE# persist / rebuild ring state
"""

import sys
from chashring import HashRing, Node, compute_migration


def build_cluster() -> HashRing:
    ring = HashRing(base_vnodes=100, replication_factor=3)
    ring.add_node(Node("node-a", weight=2, fault_domain="zone-1"))
    ring.add_node(Node("node-b", weight=1, fault_domain="zone-2"))
    ring.add_node(Node("node-c", weight=1, fault_domain="zone-3"))
    ring.add_node(Node("node-d", weight=3, fault_domain="zone-1"))
    return ring


def demo() -> None:
    keys = [f"user:{i:05d}" for i in range(10_000)]

    ring = build_cluster()
    print("=== Cluster: 4 nodes, weights a=2 b=1 c=1 d=3, RF=3, 100 base vnodes ===")
    print(f"virtual nodes on ring: {ring.vnode_count}")
    dist = ring.load_distribution(keys)
    for nid in sorted(dist):
        print(f"  primary load {nid}: {dist[nid]:5d} "
              f"({dist[nid] / len(keys):5.1%})")

    sample = ring.place("user:00042")
    print(f"\nplacement of 'user:00042': nodes={sample.nodes} "
          f"domains={sample.fault_domains} degraded={sample.degraded}")

    before = ring.map_keys(keys)

    print("\n=== Scale out: add node-e (weight=1, zone-2) ===")
    ring.add_node(Node("node-e", weight=1, fault_domain="zone-2"))
    mig = compute_migration(before, ring.map_keys(keys))
    print(f"migration: primary {mig['primary_ratio']:.2%} of keys "
          f"(ideal ~1/5=20%), replica pairs {mig['replica_pair_ratio']:.2%}, "
          f"replica-set changed {mig['ratio']:.2%}")

    print("\n=== Scale in: remove node-b ===")
    before = ring.map_keys(keys)
    ring.remove_node("node-b")
    mig = compute_migration(before, ring.map_keys(keys))
    print(f"migration: primary {mig['primary_ratio']:.2%} of keys, "
          f"replica pairs {mig['replica_pair_ratio']:.2%}, "
          f"replica-set changed {mig['ratio']:.2%}")

    print("\n=== Fault-domain degradation: shrink to one zone ===")
    single = HashRing(base_vnodes=100, replication_factor=3)
    single.add_node(Node("n1", fault_domain="only-zone"))
    single.add_node(Node("n2", fault_domain="only-zone"))
    single.add_node(Node("n3", fault_domain="only-zone"))
    p = single.place("user:00042")
    print(f"placement: nodes={p.nodes} domains={p.fault_domains} "
          f"degraded={p.degraded} (expected degraded=True)")

    print("\n=== Persistence: save / load round-trip ===")
    ring.save("ring_state.json")
    loaded = HashRing.load("ring_state.json")
    same = ring.map_keys(keys) == loaded.map_keys(keys)
    print(f"state saved to ring_state.json; reloaded mapping identical: {same}")
    print(f"history entries recorded: {len(loaded.history)}")


def main(argv) -> None:
    if len(argv) < 2 or argv[1] == "demo":
        demo()
    elif argv[1] == "place":
        ring = build_cluster()
        for key in argv[2:]:
            p = ring.place(key)
            flag = " DEGRADED" if p.degraded else ""
            print(f"{key}: {p.nodes} domains={p.fault_domains}{flag}")
    elif argv[1] == "save":
        build_cluster().save(argv[2])
        print(f"saved to {argv[2]}")
    elif argv[1] == "load":
        ring = HashRing.load(argv[2])
        print(f"loaded {len(ring.nodes)} nodes, {ring.vnode_count} vnodes")
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
