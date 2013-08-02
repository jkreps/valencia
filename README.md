# Valencia: A Log-Structured On-Disk Hash

Valencia is a simple, embeddable, pure-java disk hash.

It supports a simple put/get/delete API as well as full table scans; it does not support range scans.

It scales well: with high probability it does only a single random I/O for gets, a single random I/O for updates and only sequential I/O for inserts and deletes irrespective of data size.

It keeps 12 bytes of in-memory index per-key regardless of key size and has 8 bytes of overhead per-record on disk.