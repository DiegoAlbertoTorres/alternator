# Alternator
Alternator, my senior project, is a distributed, fault-tolerant, gossip-based key-value store for low-powered devices, aimed at the context of The Internet of Things.

The replication scheme will consist of a ring where N nodes hold each key. Each key is owned by a coordinator, which handles replication of that key. The ring's membership is gossip-based.

-Each node will be a coordinator for all keys in its range.
-Adding a node n to the ring requires the assistance of a node n':
	-Node n' changes its node history, and propagates the change through gossip.
	-As nodes are notified of new member, they can do any of the following two: delegate coordination of some keys to the new node n, or ask n to replicate some of their keys. n eventually acquires a copy of the keys its coordinates and the keys it has to replicate. For replicated keys it does nothing.
