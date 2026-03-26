# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build the project
./gradlew build

# Run the application
./gradlew run

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.flo.app.actor.SegmentProcessorTest"

# Run a single test method
./gradlew test --tests "com.flo.app.actor.SegmentProcessorTest.testOnProcessChunk"
```

## Architecture

This is a **distributed CSV file processing system** built on the **Akka Typed Actor Model** (v2.6.20). It processes NMI300 meter data from CSV files by chunking them and distributing work across a pool of actors. The output is SQL INSERT statements written to files.

### Actor Hierarchy

```
ActorSystem ("ClusterSystem")
└── WorkloadDistributor          (root orchestrator)
    ├── SegmentProcessor[0..N]   (worker pool, size = CPU count)
    └── Nmi300PersistenceActor   (persistence coordinator)
        └── FilePersistenceActor (one per input file, writes SQL output)
```

### Data Flow

1. **WorkloadDistributor** scans `input/` every 5 seconds, renames files to `*.processing` (atomic claim), splits them into byte-range chunks respecting newline boundaries, and fans out `ProcessChunk` messages to the worker pool.
2. **SegmentProcessor** reads its assigned byte range, parses lines starting with `"300,"` (NMI meter records), strips UTF-8 BOM if present, and sends `PersistNmi` messages.
3. **Nmi300PersistenceActor** tracks chunk completion state (`FileProcessingState`) per file. It lazily spawns a `FilePersistenceActor` child for each file (using stashing while creating it) and signals `FileComplete` back to `WorkloadDistributor` when all chunks are done.
4. **FilePersistenceActor** writes SQL INSERT statements to `output/<filename>.sql` and maintains an `InMemoryDataStore` cache.
5. On success, `WorkloadDistributor` moves `*.processing` → `processed/`. On failure, → `error/`. Orphaned `*.processing` files on startup are reverted to original names for retry.

### Key Configuration (`src/main/resources/application.conf`)

- `chunk-size-mb`: 1 MB (configurable chunk size)
- `input-dir` / `output-dir` / `processed-dir`: file lifecycle directories
- Akka Artery transport on `127.0.0.1:2551`; seed node: `akka://ClusterSystem@127.0.0.1:2551`
- Inter-node serialization: Jackson CBOR — all clustered messages must implement `CborSerializable`

### Supervision

`SegmentProcessor` workers use a restart-on-failure supervision strategy. `Nmi300PersistenceActor` uses stashing (via `AbstractBehaviorWithStash`) to buffer messages while child `FilePersistenceActor` instances are being created.