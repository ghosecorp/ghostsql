# Introduction

GhostSQL is a high-performance, PostgreSQL-compatible SQL database written. It is designed for modern applications that require both scalable relational data and fast vector search for AI/ML workloads.

## Vision

The goal of GhostSQL is to bridge the gap between traditional relational databases and specialized vector stores. By providing a single, unified interface that speaks the PostgreSQL protocol, GhostSQL allows developers to use their favorite tools while gaining native vector acceleration.

## Key Principles

*   **Protocol Compatibility**: Works with standard `psql` and PostgreSQL drivers.
*   **Vector First**: High-performance embeddings storage and HNSW indexing.
*   **Relational Integrity**: Support for JOINs, Foreign Keys, and ACID properties.
*   **Performance**: Written in Go with efficient on-disk binary storage.

## Use Cases

*   **RAG Applications**: Store documents and their embeddings in the same table.
*   **Semantic Search**: Build intelligent search into existing relational applications.
*   **Edge Computing**: Lightweight, single-binary database for distributed environments.
