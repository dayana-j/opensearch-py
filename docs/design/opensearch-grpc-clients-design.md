# gRPC Bulk Transport for OpenSearch Clients — Design Document

## Executive Summary

This document describes the design and implementation of gRPC transport support for the OpenSearch Python client (`opensearch-py`) and OpenSearch Java client (`opensearch-java`). The project adds high-performance bulk document ingestion over gRPC to both clients while maintaining full backward compatibility with existing REST-based workflows. Users gain significant performance improvements with minimal code changes — in most cases, a single configuration parameter.

---

## Problem Statement

OpenSearch clients currently communicate exclusively over REST (HTTP/1.1 with JSON). For bulk document ingestion — the highest-throughput operation — this introduces overhead:

- **JSON serialization/deserialization** on both client and server for every document
- **HTTP/1.1 limitations** — no multiplexing, new connections for each request in many configurations
- **Text-based protocol overhead** — JSON is verbose compared to binary formats
- **Repeated TLS handshakes** when connection pooling is not optimal

OpenSearch 3.x introduced a gRPC transport layer (`transport-grpc` module) that accepts bulk requests as Protocol Buffers over HTTP/2. This project builds the client-side support to leverage that capability.

---

## Customer Goals

### Goal 1: Easy Adoption

Make the gRPC transport available for clients to use immediately with familiar patterns. Users should not need to learn new APIs, new request formats, or new error handling patterns. The existing `bulk()` method works identically — only the underlying transport changes.

**How we achieve this:**
- The `OpenSearchGrpc` client (Python) and `HybridTransport` (Java) extend the standard client classes
- All existing methods, parameters, and response formats remain unchanged
- gRPC is activated by adding a single `grpc_hosts` parameter
- The client automatically detects which operations can use gRPC and routes accordingly

### Goal 2: Zero Migration

Allow current clients to use gRPC by making minimal changes to their existing document upload code. The transport handles protocol translation under the hood — converting the user's familiar JSON/dict-based bulk body into protobuf, sending over gRPC, and converting the protobuf response back to the standard format.

**How we achieve this:**
- The translation layer (`BulkRequestProtoBuilder` / `ResponseConverter`) handles all format conversion internally
- Users continue to pass the same bulk body format (list of dicts, NDJSON strings, or action iterables)
- Response dictionaries maintain the same structure (`took`, `errors`, `items[]`)
- Exception types are identical — `AuthenticationException`, `ConnectionError`, `RequestError`, etc.
- If gRPC is unavailable, requests silently fall back to REST without error

### Goal 3: Improved Performance

Reduce latency and increase throughput for bulk operations through:
- **Binary serialization (Protocol Buffers)** — eliminates JSON parsing overhead
- **HTTP/2 persistent connections** — single connection with multiplexing, no head-of-line blocking
- **Reduced payload size** — protobuf encoding is more compact than JSON/NDJSON
- **Server-side efficiency** — OpenSearch processes protobuf requests directly
- **Reduced metadata overhead** — gRPC headers are compressed via HPACK

Expected improvement: >20% faster bulk ingestion throughput for batches of 10K+ documents compared to REST.

---

## Architecture Overview

### Request Flow

```
User Application
       │
       │  client.bulk(body=[...])
       ▼
┌─────────────────────────────┐
│   OpenSearchGrpc (Python)   │
│   HybridTransport (Java)    │
│                             │
│   Routing Decision:         │
│   • /_bulk → gRPC           │
│   • /<index>/_bulk → gRPC   │
│   • everything else → REST  │
└──────────┬──────────────────┘
           │
     ┌─────┴─────┐
     │           │
     ▼           ▼
┌─────────┐  ┌──────────┐
│  gRPC   │  │   REST   │
│Transport│  │Transport │
│         │  │(existing)│
└────┬────┘  └────┬─────┘
     │            │
     ▼            ▼
┌─────────┐  ┌──────────┐
│OS :9400 │  │OS :9200  │
│(gRPC)   │  │(REST)    │
└─────────┘  └──────────┘
```

### Translation Layer

The translation layer is the core of the zero-migration promise. It converts between the formats users already use and the protobuf format that gRPC requires:

**Inbound (request):**
```
Python dict/NDJSON  ──→  BulkRequestProtoBuilder  ──→  Protobuf BulkRequest
Java BulkRequest    ──→  BulkRequestProtoBuilder  ──→  Protobuf BulkRequest
```

**Outbound (response):**
```
Protobuf BulkResponse  ──→  ResponseConverter  ──→  Python dict (same as REST)
Protobuf BulkResponse  ──→  ResponseConverter  ──→  Java BulkResponse (same as REST)
```

**What gets converted:**
- Index/Create/Update/Delete operations with all metadata fields
- Document source (JSON string within protobuf bytes field)
- Request-level parameters: `refresh`, `timeout`, `pipeline`, `routing`, `require_alias`
- Response fields: `took`, `errors`, `items[]` with `_index`, `_id`, `result`, `status`, `_version`, `_shards`, `_seq_no`, `_primary_term`
- gRPC status codes mapped back to HTTP status codes for client compatibility

### Channel Management

The gRPC channel is a persistent, long-lived connection that handles:

1. **Connection pooling** — HTTP/2 multiplexes multiple requests over one TCP connection
2. **Auto-reconnect** — if the channel enters `TRANSIENT_FAILURE`, gRPC retries automatically with backoff
3. **Health checking** — channel state is verified before each call; `SHUTDOWN` state triggers recreation
4. **Graceful shutdown** — `close()` method properly terminates the channel
5. **Keepalive** — configurable pings to prevent idle connection timeout

### REST Fallback

When gRPC is unavailable after retries:
1. The bulk request is re-sent via the REST transport automatically
2. No error is surfaced to the user
3. The operation succeeds transparently
4. gRPC is attempted again on the next request (not permanently disabled)

Non-retryable errors (authentication failures, bad requests) raise immediately — they do NOT fall back to REST, since the error would occur on REST as well.

---

## Authentication

All authentication methods supported by the REST client are also supported over gRPC, using the same parameters and configuration patterns.

### Basic Authentication

| Aspect | Implementation |
|--------|---------------|
| Parameter | `http_auth=("username", "password")` |
| Mechanism | gRPC `ClientInterceptor` / `UnaryUnaryClientInterceptor` |
| Header | `authorization: Basic <base64(user:pass)>` as gRPC metadata |
| Channel requirement | Works on both secure (TLS) and insecure channels |

### JWT/Bearer Token

| Aspect | Implementation |
|--------|---------------|
| Parameter | `http_auth="Bearer <jwt-token>"` |
| Mechanism | `BearerTokenInterceptor` attaches token as metadata |
| Header | `authorization: Bearer <token>` as gRPC metadata |
| Channel requirement | TLS required (per OpenSearch docs, supported since 3.5) |
| Token refresh | Not handled client-side — user provides current token |

### AWS SigV4

| Aspect | Implementation |
|--------|---------------|
| Parameter | `http_auth=Urllib3AWSV4SignerAuth(credentials, region, service)` (Python) |
| Mechanism | `AWSV4GrpcInterceptor` signs each call with SigV4 |
| Signing URL | `https://{host}/opensearch.DocumentService/Bulk` |
| Body signing | Serialized protobuf included in payload hash |
| Headers attached | `authorization`, `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token` |
| Credential refresh | `get_frozen_credentials()` called on every request |
| Services | `es` (managed OpenSearch), `aoss` (serverless) |

### Mutual TLS (mTLS)

| Aspect | Implementation |
|--------|---------------|
| Parameters | `client_cert`, `client_key` |
| Mechanism | Client certificate loaded into `ssl_channel_credentials` |
| Verification | Server verifies client identity during TLS handshake |

---

## TLS/SSL Support

### Supported Parameters

| Parameter | gRPC Mapping | Notes |
|-----------|-------------|-------|
| `use_ssl=True` | `grpc.secure_channel()` | Creates encrypted channel |
| `ca_certs` | `root_certificates` in `ssl_channel_credentials` | Server cert verification |
| `ssl_context` | CA certs extracted (DER→PEM) | Python `ssl.SSLContext` support |
| `client_cert` + `client_key` | `certificate_chain` + `private_key` | Mutual TLS |
| `ssl_assert_hostname` | `grpc.ssl_target_name_override` channel option | Hostname override |
| `ssl_version` | Accepted silently | gRPC auto-negotiates via ALPN |

### Limitations

- **`verify_certs=False` has no gRPC equivalent** — gRPC always verifies certificates. Users with self-signed certs must provide `ca_certs`. A warning is emitted when `verify_certs=False` is used without CA certs.
- **`ssl_assert_fingerprint`** — No gRPC equivalent, raises `NotImplementedError`

---

## Error Handling

### Comprehensive gRPC Status Code Mapping

Following the mapping defined in [opensearch-project/OpenSearch#18926](https://github.com/opensearch-project/OpenSearch/issues/18926):

| gRPC Status | Code | HTTP Equivalent | Client Exception | Retryable |
|-------------|------|----------------|-----------------|-----------|
| OK | 0 | 200/201 | (none) | — |
| CANCELLED | 1 | 499 | TransportError | No |
| UNKNOWN | 2 | 500 | TransportError | No |
| INVALID_ARGUMENT | 3 | 400 | RequestError | No |
| DEADLINE_EXCEEDED | 4 | 408 | ConnectionTimeout | Yes |
| NOT_FOUND | 5 | 404 | NotFoundError | No |
| ALREADY_EXISTS | 6 | 409 | ConflictError | No |
| PERMISSION_DENIED | 7 | 403 | AuthorizationException | No |
| RESOURCE_EXHAUSTED | 8 | 429 | TransportError | No |
| FAILED_PRECONDITION | 9 | 400 | RequestError | No |
| ABORTED | 10 | 409 | ConflictError | No |
| OUT_OF_RANGE | 11 | 400 | RequestError | No |
| UNIMPLEMENTED | 12 | 501 | TransportError | No |
| INTERNAL | 13 | 500 | TransportError | No |
| UNAVAILABLE | 14 | 503 | ConnectionError | Yes |
| DATA_LOSS | 15 | 500 | TransportError | No |
| UNAUTHENTICATED | 16 | 401 | AuthenticationException | No |

### Retry + Fallback Behavior

```
gRPC call fails
       │
       ▼
┌─────────────────────┐     Yes     ┌──────────────┐
│ Is error retryable? │────────────▶│ Retry (up to │
│ (UNAVAILABLE,       │             │ max_retries) │
│  DEADLINE_EXCEEDED) │             └──────┬───────┘
└─────────┬───────────┘                    │
          │ No                             │ All retries exhausted
          ▼                                ▼
┌─────────────────────┐     ┌─────────────────────────┐
│ Is error non-       │     │ Fall back to REST       │
│ retryable?          │     │ (silent, no error)      │
│ (AUTH, REQUEST, etc)│     └─────────────────────────┘
└─────────┬───────────┘
          │ Yes
          ▼
┌─────────────────────┐
│ RAISE immediately   │
│ (no REST fallback)  │
└─────────────────────┘
```

---

## Python Implementation (`opensearch-py`)

### Components

| File | Purpose |
|------|---------|
| `opensearch_grpc/grpc_transport.py` | `GrpcTransport`, interceptors, channel management |
| `opensearch_grpc/translation.py` | `BulkRequestProtoBuilder`, `ResponseConverter` |
| `opensearchpy/client/grpc_client.py` | `OpenSearchGrpc` class (generated) |
| `utils/templates/grpc_client` | Template for code generation |
| `utils/generate_api.py` | Generates `grpc_client.py` from template |

### Usage Examples

```python
from opensearchpy import OpenSearchGrpc

# Basic auth + TLS
client = OpenSearchGrpc(
    hosts=[{"host": "localhost", "port": 9200}],
    grpc_hosts=[{"host": "localhost", "port": 9400}],
    http_auth=("admin", "password"),
    use_ssl=True,
    ca_certs="/path/to/root-ca.pem",
)

# Bulk goes over gRPC — same API as before
resp = client.bulk(body=[
    {"index": {"_index": "my-index", "_id": "1"}},
    {"title": "Document 1"},
    {"index": {"_index": "my-index", "_id": "2"}},
    {"title": "Document 2"},
])

# helpers.bulk() also routes over gRPC
from opensearchpy import helpers
success, errors = helpers.bulk(client, actions)

# Search goes through REST fallback — transparent
results = client.search(index="my-index", body={"query": {"match_all": {}}})
```

### Test Coverage

| Test File | Type | What it covers |
|-----------|------|---------------|
| `test_grpc_tls_unit.py` | Unit | TLS channel creation, ssl params, param validation |
| `test_grpc_basic_auth.py` | Unit | BasicAuthInterceptor, metadata attachment |
| `test_grpc_sigv4.py` | Unit | SigV4 signing, credential scope, body hashing |
| `test_grpc_jwt.py` | Unit | BearerTokenInterceptor, token format |
| `test_grpc_exceptions.py` | Unit | Status code mapping, retry behavior |
| `test_grpc_sigv4_integration.py` | Mock server | SigV4 headers reach server correctly |
| `test_grpc_jwt_integration.py` | Mock server | Bearer tokens reach server correctly |
| `test_grpc_client_integration.py` | Mock server | Low-level + high-level client, all auth methods |
| `test_server_secured/test_grpc_secure.py` | Live server | TLS + basic auth + mTLS against OpenSearch 3.x |
| `test_server/test_grpc/test_bulk.py` | Live server | Bulk correctness (index, update, delete, mixed) |

---

## Java Implementation (`opensearch-java`)

### Components (Planned)

| Component | Purpose |
|-----------|---------|
| `GrpcTransport` | Implements `Transport` interface, manages `ManagedChannel` |
| `HybridTransport` | Composes GrpcTransport + RestClientTransport, routes by operation |
| `BulkRequestProtoBuilder` | Java `BulkRequest` → protobuf `BulkRequest` |
| `ResponseConverter` | Protobuf `BulkResponse` → Java `BulkResponse` |
| `BasicAuthInterceptor` | `ClientInterceptor` for basic auth metadata |
| `GrpcSigV4Interceptor` | `ClientInterceptor` for AWS SigV4 signing |
| `BearerTokenInterceptor` | `ClientInterceptor` for JWT tokens |

### Usage Examples (Planned)

```java
// Create hybrid transport — gRPC for bulk, REST for everything else
OpenSearchClient client = new OpenSearchClient(
    new HybridTransport(
        new RestClientTransport(restClient, new JacksonJsonpMapper()),
        GrpcTransport.builder()
            .host("localhost")
            .port(9400)
            .useSsl(true)
            .caCerts("/path/to/root-ca.pem")
            .build()
    )
);

// Same API — bulk automatically goes over gRPC
BulkResponse response = client.bulk(b -> b
    .operations(ops -> ops
        .index(i -> i.index("my-index").id("1").document(doc))
    )
);
```

---

## CI Infrastructure

### Docker Setup

The CI pipeline runs integration tests against a real OpenSearch 3.x container with gRPC enabled:

1. **Dockerfile** configures `aux.transport.types: [secure-transport-grpc]` for the gRPC module
2. **`run-opensearch.sh`** passes TLS cert settings as Docker env vars and exposes port 9400
3. **`run-repository.sh`** copies `root-ca.pem` and client certs from the container, mounts them into the test container
4. **Test runner** detects gRPC availability and skips tests gracefully when not available (pre-3.x versions)

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `OPENSEARCH_GRPC_HOST` | gRPC endpoint hostname (Docker container name) |
| `OPENSEARCH_GRPC_PORT` | gRPC port (default 9400) |
| `OPENSEARCH_CA_CERTS` | Path to root CA for TLS verification |
| `OPENSEARCH_CLIENT_CERT` | Path to client cert for mTLS |
| `OPENSEARCH_CLIENT_KEY` | Path to client key for mTLS |
| `SECURE_INTEGRATION` | Whether security plugin is enabled |

---

## Implementation Timeline after first weeks 6

### Week 1-2: Python Client (Completed)
- Translation layer (BulkRequestProtoBuilder, ResponseConverter)
- GrpcTransport with TLS/mTLS
- Basic auth, JWT, SigV4 interceptors
- Channel reconnect + REST fallback
- OpenSearchGrpc client (generated)
- Unit + integration tests

### Week 3-4: Java Client
- Java translation layer (BulkRequest → protobuf converter)
- GrpcTransport implementing Transport interface
- HybridTransport (routing + fallback)
- Auth interceptors (Basic, SigV4, JWT)
- Channel lifecycle management
- Unit + integration tests

### Week 5: Search + Performance
- Search over gRPC (if proto coverage allows)
- Performance benchmarking (REST vs gRPC)
- k-NN search over gRPC
- Documentation

### Week 6: Documentation + Final
- User guides for both clients
- Final benchmarks
- All PRs submitted and reviewed
- Demo/presentation

---

## Success Criteria

1. `OpenSearchGrpc` / `HybridTransport` routes bulk over gRPC with REST fallback
2. Auth (Basic + SigV4 + JWT) works identically over both transports
3. Channel auto-reconnects; if gRPC unavailable, falls back to REST silently
4. Users need only add `grpc_hosts` parameter — no other code changes
5. Benchmarks show >20% improvement for bulk ingestion of 10K+ doc batches
6. All CI tests pass against OpenSearch 3.x with security enabled
7. High-level helpers (`helpers.bulk()` in Python, fluent API in Java) inherit gRPC support

---

## References

- [opensearch-protobufs](https://github.com/opensearch-project/opensearch-protobufs) — Protocol Buffer definitions
- [OpenSearch gRPC APIs](https://docs.opensearch.org/latest/api-reference/grpc-apis/) — Bulk, Search, k-NN
- [OpenSearch#18926](https://github.com/opensearch-project/OpenSearch/issues/18926) — Status code mapping
- [OpenSearch JWT over gRPC](https://docs.opensearch.org/latest/security/authentication-backends/jwt/) — JWT auth (3.5+)
- [AWS SigV4 for clients](https://opensearch.org/blog/aws-sigv4-support-for-clients/) — SigV4 patterns
- [gRPC Python auth examples](https://github.com/grpc/grpc/tree/master/examples/python/auth)
- [gRPC Java examples](https://github.com/grpc/grpc-java/tree/master/examples)
- [SigV4 signing examples](https://github.com/aws-samples/sigv4-signing-examples)
