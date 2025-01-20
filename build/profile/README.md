This folder contains cpu profile files for TiDB running some standard tests. These `.proto` files will be merged into one cpu profile for PGO compilation.

The current way to generate these profile files is to grab the cpu profile of TiDB for 30 seconds after the QPS has stabilized during a performance test.

Note that these profile files will expire as code changes are made, which will affect the performance of the PGO build, so we need to update these profiles periodically.
