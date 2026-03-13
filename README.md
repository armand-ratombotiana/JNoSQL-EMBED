# JNoSQL-EMBED

> The embedded multi-model NoSQL database for the JVM.

JNoSQL-EMBED is a lightweight embedded NoSQL database written entirely
in Java that implements the Jakarta NoSQL specification.

Think of it as **H2 for the NoSQL world** --- a fast, tiny database you
can embed directly into JVM applications.

## Features

-   Multi-model: Document, Key-Value, Column Family, Vector Search
-   ACID Transactions
-   Tiny footprint (\<5MB)
-   Embedded or Server Mode
-   Pluggable storage engines (B-Tree, LSM, In-Memory)
-   Vector search using HNSW indexes
-   MVCC concurrency
-   Built-in web console
-   Encryption at rest (AES‑256)

## Installation

### Maven

`<dependency>`{=html}
`<groupId>`{=html}org.jnosql.embed`</groupId>`{=html}
`<artifactId>`{=html}jnosql-embed-core`</artifactId>`{=html}
`<version>`{=html}1.0.0`</version>`{=html} `</dependency>`{=html}

### Gradle

implementation("org.jnosql.embed:jnosql-embed-core:1.0.0")

## Quick Example

var db = JNoSQL.embed() .storageEngine(StorageEngine.LSM) .build();

DocumentCollection users = db.collection("users");

users.insert(Document.parse(
