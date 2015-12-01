# Alternator
Alternator, my senior project, is a distributed, fault-tolerant, gossip-based key-value store for
IoT networks. Its key feature is that the user can select the nodes which will store and replicate
every individual entry value in the store. This is useful in the IoT context and more generally
in heterogeneous networks because it allows the user to map keys to nodes depending on specific
features of those nodes. For example, essential data can be stored in nodes with a low disk failure
rate and data required for complex operations can be stored in nodes with powerful CPUs.

* The storage scheme is a ring, much like Chord. The coordinator of any key-value pair is the
successor of the hash of the key (the pair's name).

* Each node keeps a full membership list of the ring.

* The membership list is maintained by keeping a history of changes in the ring's membershi These
changes are propagated using a simple gossip algorith

* The coordinator of a pair stores its metadata. This includes the ID of the nodes that are in
charge of replicating the key's corresponding value. The coordinator does not store the value
itself.

This basic scheme allows Alternator to look up any key in constant time, requiring two remote
procedure calls for each Get, and N + 1 for each write, where N is the amount of replicating
nodes.
