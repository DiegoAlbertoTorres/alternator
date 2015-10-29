# Alternator
Alternator, my senior project, is a distributed, fault-tolerant, gossip-based key-value store for low-powered devices, aimed at the context of swarms of robots. It is based on Amazon's dynamo.

* The storage scheme is a ring, much like Chord, where maintaining the value of a key is up to the
successor of that key, which will be called the coordinator of that key.

* The replication scheme consists of storing the value of a key not only at its coordinator, but
also the first N successors of the coordinator. The coordinator is in charge of ensuring that the
key is properly replicated.

* Membership of the ring is gossip-based, and must be assisted by some node already in the ring.
