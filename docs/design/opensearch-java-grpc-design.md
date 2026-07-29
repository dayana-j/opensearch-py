# OpenSearch Java Client — gRPC Transport Layer Design Document

## Project Overview

This document outlines the design for adding gRPC transport support to the OpenSearch Java client (opensearch-java). The implementation follows the same architecture established in the Python client (opensearch-py) and aims to provide high-performance bulk document ingestion over gRPC while maintaining full backward compatibility with the existing REST-based Java client.

---

## Customer Goals

### 1. Easy Adoption — Available for Clients to Use Immediately

The gRPC transport should be a drop-in enhancement that clients can enable with minimal configuration. Users familiar with the existing OpenSearch Java client should be able to activate gRPC by providing a single additional parameter (the gRPC endpoint) without learning new APIs or patterns.

**Design Principle:** The existing `OpenSearchClient` API surface remains unchanged. Bulk operations are transparently routed over gRPC when a gRPC endpoint is configured. All other operations continue to use REST.

### 2. Zero Migration — Minimal Changes to Existing Code

Current clients using the Java client for document uploads should require no changes to their application logic. The gRPC transport operates under the hood — the same `bulk()` method, the same request/response objects, the same error handling. The transport layer handles the protocol translation internally.

**Design Principle:** The `HybridTransport` composes the existing HTTP transport with the new gRPC transport. Bulk requests are intercepted and routed over gRPC; all other requests pass through to HTTP unchanged. If gRPC is unavailable, bulk requests silently fall back to REST.

### 3. Improved Performance — Reduced Latency, Faster Processing

gRPC provides significant performance advantages over REST for bulk operations:
- **Binary serialization (Protocol Buffers)** eliminates JSON parsing overhead on both client and server
- **HTTP/2 multiplexing** allows multiple requests over a single connection without head-of-line blocking
- **Persistent connections** avoid repeated TLS handshakes and TCP connection setup
- **Reduced payload size** — protobuf encoding is more compact than JSON/NDJSON
- **Server-side efficiency** — OpenSearch can process protobuf bulk requests directly without JSON deserialization

Expected improvement: >20% faster bulk ingestion throughput for 10K+ document batches compared to REST.

---

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────┐
│                  User Application                     │
│                                                       │
│   OpenSearchClient.bulk(request)                     │
│   OpenSearchClient.search(request)                   │
│   OpenSearchClient.index(request)                    │
└───────────────────────┬───────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                  HybridTransport                      │
│                                                       │
│   Routes requests based on operation type:           │
│   • bulk → GrpcTransport                             │
│   • everything else → HTTP Transport (existing)      │
│                                                       │
│   Fallback: if gRPC fails → HTTP Transport           │
└──────────┬────────────────────────────┬───────────────┘
           │                            │
           ▼                            ▼
┌─────────────────────┐    ┌─────────────────────────┐
│   GrpcTransport     │    │   HTTP Transport        │
│                     │    │   (existing)            │
│  • TLS/mTLS        │    │                         │
│  • Basic Auth       │    │  • TLS/mTLS            │
│  • JWT/Bearer       │    │  • Basic Auth          │
│  • SigV4           │    │  • SigV4               │
│  • Retry + Reconnect│   │  • Connection Pool     │
│                     │    │                         │
│  ManagedChannel     │    │  Apache HttpClient     │
└─────────┬───────────┘    └────────────┬────────────┘
          │                             │
          ▼                             ▼
┌─────────────────────┐    ┌─────────────────────────┐
│  OpenSearch gRPC    │    │  OpenSearch REST API    │
│  Port 9400          │    │  Port 9200/443          │
└─────────────────────┘    └─────────────────────────┘
```

### Translation Layer

The translation layer converts between the Java client's request/response types and the protobuf types defined in opensearch-protobufs:

```
Java BulkRequest → Protobuf BulkRequest → gRPC wire → Protobuf BulkResponse → Java BulkResponse
```

**Key conversions:**
- `BulkRequest` (Java client type) → `BulkRequest` (protobuf) via `BulkRequestProtoBuilder`
- `BulkResponse` (protobuf) → `BulkResponse` (Java client type) via `ResponseConverter`
- gRPC status codes → Java client exceptions (matching REST exception types)

### Channel Management

```
┌─────────────────────────────────────┐
│           ManagedChannel            │
│                                     │
│  • Persistent across requests       │
│  • Auto-reconnect on failure        │
│  • Health check before each call    │
│  • Graceful shutdown on close()     │
│  • Keepalive configuration          │
│                                     │
│  States:                            │
│  IDLE → CONNECTING → READY          │
│  TRANSIENT_FAILURE (auto-retry)     │
│  SHUTDOWN (terminal, recreate)      │
└─────────────────────────────────────┘
```

---

## Authentication

All authentication methods mirror the REST client's existing patterns:

### Basic Authentication
- Username/password encoded as metadata on every gRPC call
- Uses a `ClientInterceptor` that attaches `authorization: Basic <base64>` metadata
- Works with both secure (TLS) and insecure channels

### JWT/Bearer Token
- Pre-obtained JWT token attached as `authorization: Bearer <token>` metadata
- OpenSearch validates the token server-side using configured signing keys or JWKS endpoint
- TLS is required when using JWT over gRPC (per OpenSearch security plugin)
- Supported since OpenSearch 3.5

### AWS SigV4
- Signs each gRPC call using AWS Signature Version 4
- Uses the gRPC service method path (e.g., `/opensearch.DocumentService/Bulk`) as the URL for signing
- Serialized protobuf body is included in the payload hash
- Credentials are refreshed on every call via `get_frozen_credentials()`
- Supports managed OpenSearch Service (`service=es`) and Serverless (`service=aoss`)

### Client Certificate (mTLS)
- Client certificate and private key loaded into the channel credentials
- Server verifies client identity during TLS handshake
- Configured via `client_cert` and `client_key` parameters

---

## TLS/SSL Support

The gRPC channel supports full TLS configuration:

| Feature | Implementation |
|---------|---------------|
| Server verification | `ca_certs` → root certificates for `SslContext` |
| Mutual TLS | `client_cert` + `client_key` → client certificate chain |
| Custom SSL context | Supported — CA certs extracted and used |
| Hostname override | `ssl_assert_hostname` → `grpc.ssl_target_name_override` channel option |
| TLS version | Auto-negotiated via ALPN (same as REST) |

**Note:** gRPC Java/Python does not support `verify_certs=False` (disabling certificate verification). Users with self-signed certificates must provide the CA certificate.

---

## Error Handling

### gRPC Status Code Mapping

All gRPC status codes are mapped to the equivalent Java client exceptions, following the comprehensive mapping from opensearch-project/OpenSearch#18926:

| gRPC Status | HTTP Equivalent | Java Exception |
|-------------|----------------|----------------|
| OK (0) | 200/201 | (no error) |
| INVALID_ARGUMENT (3) | 400 | OpenSearchException (BadRequest) |
| DEADLINE_EXCEEDED (4) | 408 | ConnectionTimeout |
| NOT_FOUND (5) | 404 | OpenSearchException (NotFound) |
| ALREADY_EXISTS (6) | 409 | OpenSearchException (Conflict) |
| PERMISSION_DENIED (7) | 403 | OpenSearchException (Forbidden) |
| RESOURCE_EXHAUSTED (8) | 429 | OpenSearchException (TooManyRequests) |
| FAILED_PRECONDITION (9) | 412 | OpenSearchException |
| ABORTED (10) | 409 | OpenSearchException (Conflict) |
| UNIMPLEMENTED (12) | 501 | OpenSearchException |
| INTERNAL (13) | 500 | OpenSearchException |
| UNAVAILABLE (14) | 503 | ConnectionError |
| UNAUTHENTICATED (16) | 401 | OpenSearchException (Unauthorized) |

### Retry Behavior

- `UNAVAILABLE` and `DEADLINE_EXCEEDED` are retried up to `max_retries` times
- After retries exhausted, bulk requests fall back to REST silently
- Non-retryable errors (`UNAUTHENTICATED`, `PERMISSION_DENIED`, `INVALID_ARGUMENT`) raise immediately — no REST fallback
- Channel is reconnected between retry attempts for `UNAVAILABLE` errors

### REST Fallback

When the gRPC channel is unavailable after all retries:
1. Bulk request falls back to the HTTP transport automatically
2. The operation succeeds via REST without user intervention
3. No error is surfaced to the application
4. gRPC is retried on the next request (not permanently disabled)

---

## Java-Specific Design Considerations

### Transport Interface

The Java client's `Transport` interface defines how requests are sent. The implementation options:

1. **`GrpcTransport` implements `Transport`** — Pure gRPC, throws on unsupported operations
2. **`HybridTransport` composes `GrpcTransport` + `RestClientTransport`** — Routes bulk to gRPC, everything else to REST

The `HybridTransport` approach is preferred as it provides seamless fallback and requires no changes to user code.

### Channel Lifecycle

Java's `ManagedChannel` provides:
- Built-in connection pooling and load balancing
- Automatic reconnection with exponential backoff
- Configurable keepalive intervals
- Graceful shutdown with `shutdown()` + `awaitTermination()`

The channel should be created eagerly in the constructor and shut down when the client is closed.

### Interceptors

Java gRPC uses `ClientInterceptor` for auth (equivalent to Python's `UnaryUnaryClientInterceptor`):
- `BasicAuthInterceptor` — attaches Basic auth metadata
- `BearerTokenInterceptor` — attaches JWT Bearer token
- `GrpcSigV4Interceptor` — signs with AWS SigV4

### Code Generation

The Java client types are generated from the OpenSearch API specification. The gRPC transport should:
- Reuse the same generated client types (no new request/response classes)
- Only add the transport layer and conversion logic
- Be addable as an optional dependency

---

## Implementation Plan

### Phase 1: Translation Layer + GrpcTransport
- Java BulkRequest → protobuf BulkRequest converter
- Protobuf BulkResponse → Java BulkResponse converter
- `GrpcTransport` with ManagedChannel lifecycle
- Plaintext + TLS support
- Unit tests for conversion

### Phase 2: HybridTransport + Auth + Connection Management
- `HybridTransport` composing GrpcTransport + RestClientTransport
- Basic auth interceptor
- SigV4 interceptor (using AWS SDK credentials)
- JWT/Bearer token interceptor
- Channel health checks and auto-reconnect
- REST fallback on gRPC failure
- Integration tests

### Phase 3: High-Level Client + Performance
- Ensure high-level client inherits gRPC support automatically
- Performance benchmarking (REST vs gRPC)
- k-NN search over gRPC (if proto coverage allows)
- Documentation and user guides

---

## Dependencies

| Dependency | Purpose |
|-----------|---------|
| `opensearch-protobufs` | Protobuf type definitions for OpenSearch APIs |
| `grpc-java` | gRPC runtime (channel, stubs, interceptors) |
| `protobuf-java` | Protocol Buffers serialization |
| `grpc-netty-shaded` | Netty-based gRPC transport (HTTP/2) |
| AWS SDK v2 (optional) | SigV4 signing for managed OpenSearch |

---

## User Experience

### Before (REST only)
```java
OpenSearchClient client = new OpenSearchClient(
    new RestClientTransport(restClient, new JacksonJsonpMapper())
);

BulkResponse response = client.bulk(b -> b
    .operations(ops -> ops
        .index(i -> i.index("my-index").id("1").document(doc))
    )
);
```

### After (gRPC enabled — same API, one config change)
```java
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

// Search still goes over REST
SearchResponse<MyDoc> search = client.search(s -> s
    .index("my-index")
    .query(q -> q.matchAll(m -> m)),
    MyDoc.class
);
```

### With SigV4 (Amazon OpenSearch Service)
```java
OpenSearchClient client = new OpenSearchClient(
    new HybridTransport(
        new AwsSdk2Transport(httpClient, host, Region.US_EAST_1, options),
        GrpcTransport.builder()
            .host(host)
            .port(9400)
            .useSsl(true)
            .interceptor(new GrpcSigV4Interceptor(credentials, region, "es"))
            .build()
    )
);
```

---

## Success Criteria

1. **Java `HybridTransport` routes bulk over gRPC** with automatic HTTP fallback
2. **Auth (Basic + SigV4 + JWT) works over gRPC** — same credentials used for both transports
3. **Channel auto-reconnects on failure** — transient failures don't require client restart
4. **If gRPC unavailable, falls back to REST silently** — operations always succeed
5. **Performance gains validated** — >20% faster bulk ingestion vs REST for 10K+ doc batches
6. **Zero migration** — existing users add one config parameter, no API changes
7. **All integration tests pass** against OpenSearch 3.x with security enabled

---

## References

- [opensearch-protobufs](https://github.com/opensearch-project/opensearch-protobufs) — Proto definitions
- [OpenSearch gRPC APIs](https://docs.opensearch.org/latest/api-reference/grpc-apis/) — Bulk, Search, k-NN
- [OpenSearch#18926](https://github.com/opensearch-project/OpenSearch/issues/18926) — gRPC status code mapping
- [opensearch-py gRPC implementation](https://github.com/opensearch-project/opensearch-py) — Python reference implementation
- [gRPC Java examples](https://github.com/grpc/grpc-java/tree/master/examples) — Auth patterns
- [AWS SigV4 signing](https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html) — Signature protocol
- [OpenSearch JWT auth](https://docs.opensearch.org/latest/security/authentication-backends/jwt/) — JWT over gRPC (3.5+)
