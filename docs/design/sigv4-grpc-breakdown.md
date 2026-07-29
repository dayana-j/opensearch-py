# SigV4 for gRPC — Implementation Breakdown

## Overview

AWS SigV4 signs every request using the HTTP method, URL path, headers, and body. For gRPC, the "URL" is the gRPC service method path (e.g., `/opensearch.DocumentService/Bulk`). This document outlines how to sign gRPC requests using the existing `AWSV4Signer` class.

## What Gets Signed

In REST:
- Method: `POST`
- URL: `https://my-domain.us-east-1.es.amazonaws.com/_bulk?refresh=true`
- Body: NDJSON bulk body
- Headers: host, x-amz-date, x-amz-content-sha256

In gRPC:
- Method: `POST` (gRPC always uses POST over HTTP/2)
- URL: `https://my-domain.us-east-1.es.amazonaws.com/opensearch.DocumentService/Bulk`
- Body: serialized protobuf request
- Headers: host, x-amz-date, x-amz-content-sha256

## gRPC Service Paths

gRPC uses HTTP/2 paths in the format: `/<package>.<Service>/<Method>`

For OpenSearch gRPC APIs:
- Bulk: `/opensearch.DocumentService/Bulk`
- Search: `/opensearch.SearchService/Search`

These paths are what the AWS server expects in the SigV4 signature.

## Implementation Plan

### File: `opensearch_grpc/grpc_transport.py`

Add a new interceptor class:

```python
class AWSV4GrpcInterceptor(grpc.UnaryUnaryClientInterceptor):
    """gRPC interceptor that signs every call with AWS SigV4.

    Constructs a synthetic URL from the gRPC endpoint and signs it using
    the same AWSV4Signer that the REST client uses. Signed headers are
    attached as gRPC metadata.
    """

    def __init__(self, credentials, region, service="es", host=None):
        from opensearchpy.helpers.signer import AWSV4Signer
        self.signer = AWSV4Signer(credentials, region, service)
        self._host = host  # e.g., "my-domain.us-east-1.es.amazonaws.com"

    def intercept_unary_unary(self, continuation, client_call_details, request):
        # gRPC method path (e.g., "/opensearch.DocumentService/Bulk")
        grpc_method = client_call_details.method

        # Construct the URL that SigV4 will sign
        # gRPC uses HTTPS, POST, and the method path as the URL path
        url = f"https://{self._host}{grpc_method}"

        # Serialize the protobuf for body signing
        body = request.SerializeToString() if hasattr(request, 'SerializeToString') else None

        # Sign the request
        signed_headers = self.signer.sign(
            method="POST",
            url=url,
            body=body,
        )

        # Attach signed headers as gRPC metadata
        metadata = list(client_call_details.metadata or [])
        for key, value in signed_headers.items():
            # gRPC metadata keys must be lowercase
            metadata.append((key.lower(), value))

        new_details = client_call_details._replace(metadata=metadata)
        return continuation(new_details, request)
```

### In `GrpcTransport.__init__`:

Detect when `http_auth` is a callable (SigV4 signer) vs a tuple (basic auth):

```python
# Read auth params
self._http_auth = kwargs.get("http_auth", None)

# After channel creation...
if self._http_auth is not None:
    if isinstance(self._http_auth, (tuple, list)):
        # Basic auth
        username, password = self._http_auth[0], self._http_auth[1]
        interceptor = BasicAuthInterceptor(username, password)
    elif callable(self._http_auth):
        # SigV4 or custom callable signer
        # Need to determine if it's a Urllib3AWSV4SignerAuth
        from opensearchpy.helpers.signer import Urllib3AWSV4SignerAuth, AWSV4SignerAuth
        if hasattr(self._http_auth, 'signer'):
            # It's an AWSV4 signer — use gRPC interceptor
            interceptor = AWSV4GrpcInterceptor(
                credentials=self._http_auth.signer.credentials,
                region=self._http_auth.signer.region,
                service=self._http_auth.signer.service,
                host=grpc_host,  # the gRPC endpoint host
            )
        else:
            # Generic callable — not supported for gRPC
            raise NotImplementedError(
                "Custom callable auth is not yet supported for gRPC. "
                "Use Urllib3AWSV4SignerAuth or http_auth=('user', 'pass')."
            )
    else:
        # String format "user:pass"
        username, password = str(self._http_auth).split(":", 1)
        interceptor = BasicAuthInterceptor(username, password)

    self._channel = grpc.intercept_channel(self._channel, interceptor)
```

### Usage

```python
from boto3 import Session
from opensearchpy import Urllib3AWSV4SignerAuth
from opensearchpy.client import OpenSearchGrpc

credentials = Session().get_credentials()
auth = Urllib3AWSV4SignerAuth(credentials, region="us-east-1", service="es")

client = OpenSearchGrpc(
    hosts=[{"host": "my-domain.us-east-1.es.amazonaws.com", "port": 443}],
    grpc_hosts=[{"host": "my-domain.us-east-1.es.amazonaws.com", "port": 9400}],
    http_auth=auth,
    use_ssl=True,
)

# Bulk goes over gRPC — signed with SigV4
client.bulk(body=[...])

# Search goes over REST — also signed with SigV4 (existing behavior)
client.search(index="my-index", body={...})
```

## Key Differences from REST

| Aspect | REST | gRPC |
|--------|------|------|
| Method | Varies (GET, POST, PUT, DELETE) | Always POST |
| Path | REST endpoint (/_bulk, /_search) | gRPC method (/opensearch.DocumentService/Bulk) |
| Body | JSON/NDJSON | Serialized protobuf |
| Headers delivery | HTTP headers | gRPC metadata |
| Signing per-request | Yes (callable invoked each time) | Yes (interceptor invoked each time) |

## Challenges

1. **Body signing**: SigV4 hashes the body. For gRPC, the body is a serialized protobuf. Need to call `request.SerializeToString()` to get the bytes for signing.

2. **Host header**: SigV4 requires the `host` header to match the endpoint. For gRPC, this should be the gRPC endpoint hostname.

3. **Credentials refresh**: AWS temporary credentials expire. The interceptor must call `sign()` on every request (not cache signatures). The existing `AWSV4Signer` handles this via `get_frozen_credentials()`.

4. **Service name**: Must be `"es"` for managed OpenSearch or `"aoss"` for serverless — same as REST.

5. **TLS required**: SigV4 with AWS services requires HTTPS. The gRPC channel must use `grpc.secure_channel` (TLS) when using SigV4.

## Dependencies

- `boto3` / `botocore` (for `SigV4Auth`, `AWSRequest`)
- These are already optional dependencies in opensearch-py
- The existing `AWSV4Signer` class handles all the crypto — we just call `.sign()`

## Testing

Unit tests:
- Verify interceptor attaches correct metadata headers
- Verify signing URL uses gRPC method path
- Verify body is serialized before signing
- Verify credentials refresh on each call

Integration tests (requires AWS OpenSearch):
- Bulk over gRPC with SigV4 to managed OpenSearch
- Verify AuthenticationException on invalid credentials
- Verify REST fallback also uses SigV4


---

## Existing SigV4 Test Files (reference for gRPC tests)

### File Paths

| File | What it tests |
|------|--------------|
| `test_opensearchpy/test_connection/test_urllib3_http_connection.py` (lines 191-270, 403-430) | Urllib3 SigV4 signer: headers added, region/credentials validation, service name, frozen credentials |
| `test_opensearchpy/test_connection/test_requests_http_connection.py` (lines 464-720) | Requests SigV4 signer: URL with querystring, custom headers, content SHA256, frozen credentials, URL consistency |
| `test_opensearchpy/test_async/test_signer.py` | Async signer: headers, region/credentials validation, service name, frozen credentials, URL reconstruction |

### Key Testing Patterns to Reuse

**1. Mock credentials (from `test_urllib3_http_connection.py` line 263):**
```python
def mock_session(self):
    access_key = uuid.uuid4().hex
    secret_key = uuid.uuid4().hex
    token = uuid.uuid4().hex
    dummy_session = Mock()
    dummy_session.access_key = access_key
    dummy_session.secret_key = secret_key
    dummy_session.token = token
    del dummy_session.get_frozen_credentials
    return dummy_session
```

**2. Verify signed headers are present (from line 191):**
```python
def test_aws_signer_as_http_auth_adds_headers(self):
    auth = Urllib3AWSV4SignerAuth(self.mock_session(), "us-west-2")
    con = Urllib3HttpConnection(http_auth=auth)
    con.perform_request("GET", "/")
    headers = mock_open.call_args[1]["headers"]
    self.assertTrue(headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential="))
    self.assertIn("X-Amz-Date", headers)
    self.assertIn("X-Amz-Security-Token", headers)
    self.assertIn("X-Amz-Content-SHA256", headers)
```

**3. Validate error cases (from line 220):**
```python
def test_aws_signer_when_region_is_null(self):
    with pytest.raises(ValueError) as e:
        Urllib3AWSV4SignerAuth(session, None)
    self.assertEqual(str(e.value), "Region cannot be empty")
```

**4. Frozen credentials test (from line 416):**
```python
def test_frozen_credentials(self):
    mock_session = self.mock_session()  # with get_frozen_credentials
    auth = Urllib3AWSV4SignerAuth(mock_session, region)
    headers = auth("GET", "http://localhost", None)
    mock_session.get_frozen_credentials.assert_called_once()
```

### Proposed gRPC SigV4 Unit Tests

Based on the existing patterns, the gRPC tests would be:

```python
# test_opensearchpy/test_grpc_sigv4.py

class TestAWSV4GrpcInterceptor(TestCase):
    """Unit tests for AWSV4GrpcInterceptor."""

    def mock_session(self):
        """Reuse the same mock pattern as existing tests."""
        access_key = uuid.uuid4().hex
        secret_key = uuid.uuid4().hex
        token = uuid.uuid4().hex
        dummy_session = Mock()
        dummy_session.access_key = access_key
        dummy_session.secret_key = secret_key
        dummy_session.token = token
        del dummy_session.get_frozen_credentials
        return dummy_session

    def test_interceptor_adds_authorization_metadata(self):
        """Verify SigV4 headers are added as gRPC metadata."""
        # Similar to test_aws_signer_as_http_auth_adds_headers

    def test_interceptor_signs_with_grpc_method_path(self):
        """Verify the signing URL uses the gRPC method path."""
        # Verify sign() is called with URL like:
        # https://host/opensearch.DocumentService/Bulk

    def test_interceptor_signs_body(self):
        """Verify serialized protobuf body is included in signature."""
        # X-Amz-Content-SHA256 should hash the protobuf bytes

    def test_interceptor_region_validation(self):
        """Region cannot be empty."""
        # Reuse pattern from test_aws_signer_when_region_is_null

    def test_interceptor_credentials_validation(self):
        """Credentials cannot be empty."""
        # Reuse pattern from test_aws_signer_when_credentials_is_null

    def test_interceptor_service_name(self):
        """Service name is passed through (es vs aoss)."""
        # Reuse pattern from test_aws_signer_when_service_is_specified

    def test_interceptor_frozen_credentials(self):
        """get_frozen_credentials is called if available."""
        # Reuse pattern from TestSignerWithFrozenCredentials

    def test_interceptor_preserves_existing_metadata(self):
        """Existing gRPC metadata is not overwritten."""
        # Similar to how REST preserves existing headers

    def test_interceptor_signs_per_request(self):
        """Each call generates a fresh signature (no caching)."""
        # Call interceptor twice, verify different X-Amz-Date values

    def test_transport_detects_callable_auth_as_sigv4(self):
        """GrpcTransport recognizes Urllib3AWSV4SignerAuth and creates interceptor."""
        # Verify the interceptor dispatch logic
```

### Proposed gRPC SigV4 Integration Tests

```python
# test_opensearchpy/test_server_secured/test_grpc_sigv4.py
# (requires AWS OpenSearch with SigV4 auth enabled)

class TestGrpcSigV4(TestCase):
    """Integration tests for SigV4 over gRPC — requires AWS OpenSearch."""

    def test_bulk_with_valid_sigv4(self):
        """Bulk request succeeds with valid AWS credentials."""

    def test_bulk_with_invalid_credentials(self):
        """Bulk with wrong credentials raises AuthenticationException."""

    def test_rest_fallback_also_uses_sigv4(self):
        """Non-bulk operations use SigV4 via REST (existing behavior)."""
```

Note: Integration tests for SigV4 require an actual AWS OpenSearch domain (managed or serverless). They cannot run against a local Docker instance since SigV4 auth is an AWS-specific feature.
