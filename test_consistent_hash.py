#!/usr/bin/env python3
"""Terminal-runnable test suite for the consistent hash ring.

Run:  python3 -m unittest test_consistent_hash -v
   or python3 test_consistent_hash.py
"""

import os
import random
import tempfile
import unittest

from chashring import HashRing, Node, compute_migration

KEYS = [f"key-{i:06d}" for i in range(5000)]


def make_ring(n=4, rf=3, base=100, weight_fn=None, domain_fn=None):
    ring = HashRing(base_vnodes=base, replication_factor=rf)
    for i in range(n):
        nid = f"n{i}"
        ring.add_node(Node(
            nid,
            weight=weight_fn(i) if weight_fn else 1,
            fault_domain=domain_fn(i) if domain_fn else f"zone-{i}",
        ))
    return ring


class TestMembership(unittest.TestCase):
    def test_add_remove_and_lookup(self):
        ring = make_ring(3, rf=1)
        for k in KEYS[:200]:
            self.assertIn(ring.get_node(k), ring.nodes)
        ring.remove_node("n1")
        self.assertNotIn("n1", ring.nodes)
        for k in KEYS[:200]:
            self.assertIn(ring.get_node(k), ring.nodes)

    def test_duplicate_add_rejected(self):
        ring = make_ring(2)
        with self.assertRaises(ValueError):
            ring.add_node(Node("n0"))

    def test_remove_missing_rejected(self):
        ring = make_ring(2)
        with self.assertRaises(KeyError):
            ring.remove_node("ghost")

    def test_empty_ring_raises(self):
        with self.assertRaises(RuntimeError):
            HashRing().place("k")

    def test_invalid_config_rejected(self):
        with self.assertRaises(ValueError):
            HashRing(base_vnodes=0)
        with self.assertRaises(ValueError):
            HashRing(replication_factor=0)
        with self.assertRaises(ValueError):
            Node("x", weight=0)
        with self.assertRaises(ValueError):
            Node("")


class TestVirtualNodesAndWeights(unittest.TestCase):
    def test_vnode_count_scales_with_weight(self):
        ring = HashRing(base_vnodes=40)
        ring.add_node(Node("light", weight=1))
        ring.add_node(Node("heavy", weight=3))
        self.assertEqual(ring.vnode_count, 40 * 1 + 40 * 3)

    def test_weighted_load_distribution(self):
        ring = make_ring(2, rf=1, base=200, weight_fn=lambda i: (3, 1)[i])
        dist = ring.load_distribution(KEYS)
        heavy, light = dist["n0"], dist["n1"]
        ratio = heavy / light
        # Expect roughly 3:1; allow generous statistical slack.
        self.assertGreater(ratio, 1.8, f"weight ratio not reflected: {dist}")
        self.assertLess(ratio, 5.0, f"weight ratio not reflected: {dist}")

    def test_more_vnodes_improve_balance(self):
        def imbalance(base):
            ring = make_ring(6, rf=1, base=base)
            dist = ring.load_distribution(KEYS)
            mean = len(KEYS) / 6
            return max(abs(c - mean) / mean for c in dist.values())

        self.assertLess(imbalance(200), imbalance(5))


class TestReplication(unittest.TestCase):
    def test_replicas_on_distinct_nodes(self):
        ring = make_ring(5, rf=3)
        for k in KEYS[:500]:
            p = ring.place(k)
            self.assertEqual(len(p.nodes), 3)
            self.assertEqual(len(set(p.nodes)), 3, "replica on same node twice")

    def test_rf_larger_than_nodes_rejected(self):
        ring = make_ring(2, rf=1)
        with self.assertRaises(ValueError):
            ring.place("k", replication_factor=3)

    def test_rf_equals_node_count(self):
        ring = make_ring(3, rf=3)
        for k in KEYS[:100]:
            self.assertEqual(len(ring.place(k).nodes), 3)


class TestFaultDomains(unittest.TestCase):
    def test_replicas_spread_across_domains(self):
        ring = make_ring(6, rf=3, domain_fn=lambda i: f"zone-{i % 3}")
        for k in KEYS[:500]:
            p = ring.place(k)
            self.assertFalse(p.degraded)
            self.assertEqual(p.distinct_domains, 3)

    def test_degraded_when_domains_insufficient(self):
        ring = make_ring(3, rf=3, domain_fn=lambda i: "only-zone")
        p = ring.place("some-key")
        self.assertTrue(p.degraded)
        self.assertEqual(p.distinct_domains, 1)
        self.assertEqual(len(set(p.nodes)), 3, "still 3 distinct nodes")

    def test_partial_degradation_reported(self):
        # RF=3 but only 2 fault domains exist -> must report degraded,
        # while still spreading across both available domains.
        ring = make_ring(4, rf=3, domain_fn=lambda i: ("z1", "z1", "z2", "z2")[i])
        for k in KEYS[:500]:
            p = ring.place(k)
            self.assertTrue(p.degraded)
            self.assertEqual(p.distinct_domains, 2)
            self.assertEqual(len(set(p.nodes)), 3)


class TestDeterminism(unittest.TestCase):
    def test_insertion_order_irrelevant(self):
        nodes = [Node(f"n{i}", weight=(i % 3) + 1, fault_domain=f"z{i % 2}")
                 for i in range(8)]
        ring_a = HashRing(base_vnodes=50, replication_factor=3)
        ring_b = HashRing(base_vnodes=50, replication_factor=3)
        for n in nodes:
            ring_a.add_node(n)
        for n in reversed(nodes):
            ring_b.add_node(n)
        self.assertEqual(ring_a._points, ring_b._points)
        self.assertEqual(ring_a._owners, ring_b._owners)
        self.assertEqual(ring_a.map_keys(KEYS[:1000]), ring_b.map_keys(KEYS[:1000]))

    def test_shuffled_order_irrelevant(self):
        nodes = [Node(f"n{i}", fault_domain=f"z{i}") for i in range(10)]
        maps = []
        for seed in range(5):
            ring = HashRing(base_vnodes=30, replication_factor=2)
            shuffled = nodes[:]
            random.Random(seed).shuffle(shuffled)
            for n in shuffled:
                ring.add_node(n)
            maps.append(ring.map_keys(KEYS[:500]))
        for m in maps[1:]:
            self.assertEqual(maps[0], m)

    def test_repeated_runs_identical(self):
        r1, r2 = make_ring(5, rf=2), make_ring(5, rf=2)
        self.assertEqual(r1.map_keys(KEYS[:500]), r2.map_keys(KEYS[:500]))


class TestExtremeSizes(unittest.TestCase):
    def test_single_node(self):
        ring = make_ring(1, rf=1)
        for k in KEYS[:200]:
            self.assertEqual(ring.get_node(k), "n0")

    def test_single_node_rf1_only(self):
        ring = make_ring(1, rf=1)
        with self.assertRaises(ValueError):
            ring.place("k", replication_factor=2)

    def test_many_nodes(self):
        ring = make_ring(200, rf=3, base=10,
                         domain_fn=lambda i: f"z{i % 10}")
        self.assertEqual(ring.vnode_count, 2000)
        for k in KEYS[:300]:
            p = ring.place(k)
            self.assertEqual(len(set(p.nodes)), 3)
            self.assertFalse(p.degraded)

    def test_shrink_to_zero(self):
        ring = make_ring(3, rf=1)
        for i in range(3):
            ring.remove_node(f"n{i}")
        self.assertEqual(ring.vnode_count, 0)
        with self.assertRaises(RuntimeError):
            ring.place("k")


class TestMigration(unittest.TestCase):
    def test_add_node_bounded_migration(self):
        ring = make_ring(5, rf=1, base=200)
        before = ring.map_keys(KEYS)
        ring.add_node(Node("n5", fault_domain="z5"))
        mig = compute_migration(before, ring.map_keys(KEYS))
        # Ideal for primary keys: 1/6 ~= 16.7%. Must stay well under 50%.
        self.assertLess(mig["ratio"], 0.40)
        self.assertGreater(mig["ratio"], 0.02)

    def test_remove_node_bounded_migration(self):
        ring = make_ring(5, rf=1, base=200)
        before = ring.map_keys(KEYS)
        ring.remove_node("n2")
        mig = compute_migration(before, ring.map_keys(KEYS))
        self.assertLess(mig["ratio"], 0.40)
        self.assertGreater(mig["ratio"], 0.02)

    def test_replicated_migration_mostly_hits_owner(self):
        ring = make_ring(6, rf=3, base=200)
        before = ring.map_keys(KEYS)
        ring.add_node(Node("n6", fault_domain="z6"))
        mig = compute_migration(before, ring.map_keys(KEYS))
        # Replica-pair churn should be a small fraction of all pairs.
        self.assertLess(mig["replica_pair_ratio"], 0.35)

    def test_unrelated_keys_untouched(self):
        ring = make_ring(4, rf=1, base=200)
        before = ring.map_keys(KEYS)
        ring.add_node(Node("n4", fault_domain="z4"))
        after = ring.map_keys(KEYS)
        mig = compute_migration(before, after)
        # Keys that moved must now live on the new node.
        for k in mig["moved"]:
            self.assertEqual(after[k][0], "n4")

    def test_migration_empty_and_identical(self):
        self.assertEqual(compute_migration({}, {})["ratio"], 0.0)
        m = {"a": ["n0"], "b": ["n1"]}
        self.assertEqual(compute_migration(m, dict(m))["moved_keys"], 0)


class TestPersistenceAndHistory(unittest.TestCase):
    def test_save_load_roundtrip(self):
        ring = make_ring(4, rf=3, base=60,
                         weight_fn=lambda i: i + 1,
                         domain_fn=lambda i: f"z{i % 2}")
        before = ring.map_keys(KEYS[:1000])
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ring.json")
            ring.save(path)
            loaded = HashRing.load(path)
        self.assertEqual(before, loaded.map_keys(KEYS[:1000]))
        self.assertEqual(ring.vnode_count, loaded.vnode_count)

    def test_history_recorded(self):
        ring = make_ring(2, rf=1)
        ring.add_node(Node("n2"))
        ring.remove_node("n0")
        ops = [(h["op"], h["node"]) for h in ring.history]
        self.assertEqual(ops, [("add", "n0"), ("add", "n1"),
                               ("add", "n2"), ("remove", "n0")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
