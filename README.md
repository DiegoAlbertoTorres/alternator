# Alternator
Alternator, my senior project, is a distributed, fault-tolerant, gossip-based key-value store for
IoT networks. Its key feature is that the user can select the nodes which will store and replicate
every individual entry value in the store.

* The storage scheme is a ring, much like Chord. The coordinator of any key-value pair is the
successor of the hash of the key (the pair's name).

* Each node keeps a full membership list of the ring, which is propagated with a simple gossip
algorithm. Membership changes have to be explicitly made by an administrator, node failures are
assumed to be temporary.

* The coordinator of a pair stores its metadata. This includes the ID of the nodes that are in
charge of replicating the key's corresponding value. The coordinator does not store the value
itself.

This basic scheme allows Alternator to look up any key in constant time, requiring two remote
procedure calls for each Get, and N + 1 for each write, where N is the amount of replicating
nodes.
