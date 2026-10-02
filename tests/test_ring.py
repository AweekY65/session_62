from __future__ import annotations

import subprocess
import sys
from collections import Counter
from pathlib import Path
import tempfile
import unittest

from shard_sim import HashRing, Node, ShardSimulator, compare_placements


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class HashRingTests(unittest.TestCase):
    def test_basic_mapping_and_virtual_nodes(self) -> None:
        ring = HashRing([Node("a"), Node("b"), Node("c")], virtual_nodes=64)
        placement = ring.place_key("user:1")

        self.assertEqual(len(placement.replicas), 1)
        self.assertIn(placement.node_ids[0], {"a", "b", "c"})
        self.assertEqual(ring.virtual_node_count("a"), 64)
        self.assertEqual(ring.virtual_node_count("b"), 64)
        self.assertEqual(len(ring.virtual_nodes()), 192)

    def test_input_order_is_deterministic(self) -> None:
        nodes = [
            Node("a", fault_domain="zone-a"),
            Node("b", fault_domain="zone-b"),
            Node("c", fault_domain="zone-c"),
            Node("d", fault_domain="zone-d"),
        ]
        keys = [f"key-{index}" for index in range(100)]

        forward = HashRing(nodes, virtual_nodes=128, replication_factor=3)
        reverse = HashRing(list(reversed(nodes)), virtual_nodes=128, replication_factor=3)

        self.assertEqual(forward.place_keys(keys), reverse.place_keys(keys))

    def test_weight_assigns_deterministic_virtual_node_count(self) -> None:
        ring = HashRing(
            [Node("small", weight=1), Node("medium", weight=2), Node("large", weight=4)],
            virtual_nodes=50,
        )

        self.assertEqual(ring.virtual_node_count("small"), 50)
        self.assertEqual(ring.virtual_node_count("medium"), 100)
        self.assertEqual(ring.virtual_node_count("large"), 200)

        reordered = HashRing(
            [Node("large", weight=4), Node("small", weight=1), Node("medium", weight=2)],
            virtual_nodes=50,
        )
        self.assertEqual(
            [vnode.token for vnode in ring.virtual_nodes()],
            [vnode.token for vnode in reordered.virtual_nodes()],
        )

    def test_weighted_distribution_roughly_follows_weight(self) -> None:
        ring = HashRing(
            [Node("w1", weight=1), Node("w4", weight=4)],
            virtual_nodes=200,
        )
        placements = ring.place_keys([f"key:{index}" for index in range(2000)])
        owners = Counter(placement.node_ids[0] for placement in placements.values())

        self.assertGreater(owners["w4"], owners["w1"] * 2)

    def test_replicas_use_distinct_physical_nodes(self) -> None:
        ring = HashRing(
            [Node(f"node-{index}", fault_domain=f"zone-{index}") for index in range(5)],
            virtual_nodes=64,
            replication_factor=3,
        )
        placement = ring.place_key("replicated")

        self.assertEqual(len(placement.node_ids), 3)
        self.assertEqual(len(set(placement.node_ids)), 3)
        self.assertFalse(placement.degraded)

    def test_replication_larger_than_cluster_reports_degradation(self) -> None:
        ring = HashRing(
            [Node("a"), Node("b")],
            virtual_nodes=32,
            replication_factor=3,
        )
        placement = ring.place_key("small-cluster")

        self.assertEqual(set(placement.node_ids), {"a", "b"})
        self.assertEqual(len(placement.node_ids), 2)
        self.assertTrue(placement.degraded)
        self.assertIn("not_enough_physical_nodes", placement.warnings)

    def test_fault_domains_are_preferred_and_degradation_is_reported(self) -> None:
        spread = HashRing(
            [
                Node("a", fault_domain="zone-a"),
                Node("b", fault_domain="zone-b"),
                Node("c", fault_domain="zone-c"),
            ],
            virtual_nodes=32,
            replication_factor=3,
        )
        spread_placement = spread.place_key("spread")
        self.assertEqual(
            {replica.fault_domain for replica in spread_placement.replicas},
            {"zone-a", "zone-b", "zone-c"},
        )
        self.assertFalse(spread_placement.degraded)

        degraded = HashRing(
            [
                Node("a", fault_domain="zone-a"),
                Node("b", fault_domain="zone-a"),
                Node("c", fault_domain="zone-b"),
            ],
            virtual_nodes=32,
            replication_factor=3,
        ).place_key("degraded")

        self.assertEqual(len(set(degraded.node_ids)), 3)
        self.assertTrue(degraded.degraded)
        self.assertIn("fault_domain_degraded", degraded.warnings)

    def test_unlabeled_nodes_share_default_domain_and_report_degradation(self) -> None:
        ring = HashRing(
            [Node("a"), Node("b"), Node("c", fault_domain="zone-c")],
            virtual_nodes=32,
            replication_factor=3,
        )
        placement = ring.place_key("unlabeled")
        self.assertEqual(len(set(placement.node_ids)), 3)
        self.assertTrue(placement.degraded)
        self.assertIn("fault_domain_degraded", placement.warnings)

    def test_adding_node_moves_approximately_only_expected_replicas(self) -> None:
        keys = [f"key-{index}" for index in range(2000)]
        old = HashRing([Node("a"), Node("b"), Node("c")], virtual_nodes=200)
        new = HashRing(
            [Node("a"), Node("b"), Node("c"), Node("d")],
            virtual_nodes=200,
        )

        summary = compare_placements(old.place_keys(keys), new.place_keys(keys))
        self.assertAlmostEqual(summary.moved_replica_ratio, 0.25, delta=0.04)
        self.assertLessEqual(summary.changed_key_ratio, 0.30)

    def test_weighted_add_node_migration_follows_virtual_node_share(self) -> None:
        keys = [f"key-{index}" for index in range(3000)]
        old = HashRing(
            [Node("a"), Node("b"), Node("c"), Node("d")],
            virtual_nodes=200,
        )
        new = HashRing(
            [Node("a"), Node("b"), Node("c"), Node("d"), Node("heavy", weight=3)],
            virtual_nodes=200,
        )

        summary = compare_placements(old.place_keys(keys), new.place_keys(keys))
        self.assertAlmostEqual(summary.moved_replica_ratio, 3 / 7, delta=0.05)

    def test_replicated_add_node_migration_keeps_replicas_distinct(self) -> None:
        keys = [f"key-{index}" for index in range(3000)]
        old_nodes = [
            Node(f"node-{index}", fault_domain=f"zone-{index}") for index in range(5)
        ]
        new_nodes = [
            Node(f"node-{index}", fault_domain=f"zone-{index}") for index in range(6)
        ]
        old = HashRing(old_nodes, virtual_nodes=160, replication_factor=3)
        new = HashRing(new_nodes, virtual_nodes=160, replication_factor=3)
        new_placements = new.place_keys(keys)

        summary = compare_placements(old.place_keys(keys), new_placements)
        self.assertAlmostEqual(summary.moved_replica_ratio, 1 / 6, delta=0.03)
        self.assertTrue(
            all(
                len(set(placement.node_ids)) == 3
                and not placement.degraded
                for placement in new_placements.values()
            )
        )

    def test_removing_node_only_moves_its_keys_for_replication_factor_one(self) -> None:
        keys = [f"key-{index}" for index in range(1000)]
        old_ring = HashRing(
            [Node("a"), Node("b"), Node("c"), Node("d")],
            virtual_nodes=200,
        )
        old = old_ring.place_keys(keys)
        moved_keys = {
            key for key, placement in old.items() if placement.node_ids[0] == "a"
        }

        new_ring = HashRing([Node("b"), Node("c"), Node("d")], virtual_nodes=200)
        summary = compare_placements(old, new_ring.place_keys(keys))

        self.assertEqual({item.key for item in summary.keys}, moved_keys)
        for item in summary.keys:
            self.assertNotIn("a", item.new_nodes)

    def test_extreme_node_counts(self) -> None:
        empty = HashRing().place_key("empty")
        self.assertEqual(empty.replicas, ())
        self.assertTrue(empty.degraded)
        self.assertIn("ring_has_no_nodes", empty.warnings)

        one = HashRing([Node("only")], virtual_nodes=4).place_key("one")
        self.assertEqual(one.node_ids, ("only",))

        many_ring = HashRing(
            [Node(f"n-{index:04d}") for index in range(500)],
            virtual_nodes=2,
        )
        placement = many_ring.place_key("many")
        self.assertEqual(len(many_ring.virtual_nodes()), 1000)
        self.assertEqual(len(set(placement.node_ids)), 1)


class SimulatorTests(unittest.TestCase):
    def test_dynamic_topology_records_migration(self) -> None:
        simulator = ShardSimulator(
            [Node("a"), Node("b"), Node("c")],
            virtual_nodes=64,
        )
        simulator.place_keys([f"key-{index}" for index in range(100)])
        before = simulator.placements()

        summary = simulator.add_node(Node("d"))

        self.assertEqual(summary, compare_placements(before, simulator.placements()))
        self.assertGreater(summary.moved_replica_count, 0)
        self.assertAlmostEqual(summary.moved_replica_ratio, 1 / 4, delta=0.08)
        self.assertEqual(simulator.history()[-1].action, "add_node")

    def test_remove_node_preserves_no_replica_on_removed_node(self) -> None:
        simulator = ShardSimulator(
            [Node(f"node-{index}") for index in range(4)],
            virtual_nodes=32,
            replication_factor=2,
        )
        keys = [f"key-{index}" for index in range(100)]
        simulator.place_keys(keys)

        summary = simulator.remove_node("node-0")

        self.assertEqual(summary.common_key_count, len(keys))
        for placement in simulator.placements().values():
            self.assertNotIn("node-0", placement.node_ids)
            self.assertLessEqual(len(placement.replicas), 2)

    def test_local_state_round_trip_is_deterministic(self) -> None:
        simulator = ShardSimulator(
            [Node("b", weight=2, fault_domain="z2"), Node("a", fault_domain="z1")],
            virtual_nodes=37,
            replication_factor=2,
        )
        simulator.place_keys(["beta", "alpha"])

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            simulator.save_state(state_path)
            first_payload = state_path.read_text(encoding="utf-8")

            loaded = ShardSimulator.load_state(state_path)
            loaded.save_state(state_path)
            second_payload = state_path.read_text(encoding="utf-8")

        self.assertEqual(first_payload, second_payload)
        self.assertEqual(loaded.placements(), simulator.placements())
        self.assertEqual(loaded.history(), simulator.history())

    def test_replace_nodes_in_any_order_gives_same_result(self) -> None:
        nodes = [Node(f"n{index}", fault_domain=f"zone-{index % 3}") for index in range(6)]
        keys = [f"key-{index}" for index in range(50)]

        first = ShardSimulator(nodes, virtual_nodes=64, replication_factor=2)
        first.place_keys(keys)
        first.replace_nodes(list(reversed(nodes)))

        second = ShardSimulator(
            list(reversed(nodes)),
            virtual_nodes=64,
            replication_factor=2,
        )
        second.place_keys(keys)
        second.replace_nodes(nodes)

        self.assertEqual(first.placements(), second.placements())


class CliTests(unittest.TestCase):
    def test_cli_local_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            base = [
                sys.executable,
                "-m",
                "shard_sim",
                "--state",
                str(state_path),
            ]

            init = subprocess.run(
                [
                    *base,
                    "init",
                    "--virtual-nodes",
                    "32",
                    "--replication-factor",
                    "2",
                    "--node",
                    "a:1:zone-a",
                    "--node",
                    "b:2:zone-b",
                ],
                cwd=PROJECT_ROOT,
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertIn('"nodes": 2', init.stdout)

            place = subprocess.run(
                [*base, "place", "hello"],
                cwd=PROJECT_ROOT,
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertIn("hello", place.stdout)

            add = subprocess.run(
                [*base, "add-node", "c", "--weight", "3", "--fault-domain", "zone-c"],
                cwd=PROJECT_ROOT,
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertIn("moved_replica_ratio", add.stdout)

            loaded = ShardSimulator.load_state(state_path)
            self.assertEqual(loaded.virtual_nodes, 32)
            self.assertEqual(loaded.replication_factor, 2)
            self.assertEqual(loaded.ring().get_node("c").weight, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
