"""Local consistent-hashing sharding & replica placement simulator.

Pure in-memory / local-file implementation. No etcd, Consul, databases,
cloud services or any external dependencies.
"""

from .ring import HashRing, Node, Placement, compute_migration

__all__ = ["HashRing", "Node", "Placement", "compute_migration"]
__version__ = "1.0.0"
