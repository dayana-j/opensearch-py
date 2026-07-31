"""
grpc_transport.py — gRPC Transport for the opensearch-py Client

Routes bulk operations over gRPC for improved performance.
ML prediction and agent execution are streamed over gRPC.
All other operations (search, index, create, delete, update, count, etc.)
fall back to REST automatically.

    - bulk → DocumentService.Bulk (native gRPC)
    - predict_model_stream → MLService.PredictModelStream (gRPC streaming)
    - execute_agent_stream → MLService.ExecuteAgentStream (gRPC streaming)
    - everything else → REST

TLS/SSL Support:
    The gRPC channel supports TLS and mutual TLS (mTLS) using the same
    parameters as the REST client:

    - use_ssl=True: Creates a secure gRPC channel (grpc.secure_channel)
    - ssl_context: A Python ssl.SSLContext — CA certs are extracted from it
      and used for server verification. When provided, ca_certs is ignored.
    - ca_certs: Path to CA bundle for server certificate verification
    - client_cert: Path to client certificate for mTLS
    - client_key: Path to client private key for mTLS

    When use_ssl=True without ca_certs or ssl_context, system default
    trusted CAs are used.
    When use_ssl=False (default), an insecure channel is created.

    Not supported (no gRPC equivalent):
    - ssl_assert_fingerprint: Not available in gRPC Python
    - ssl_show_warn: No equivalent in gRPC

Usage:
    from opensearchpy.client import OpenSearchGrpc

    # Insecure (no TLS)
    client = OpenSearchGrpc(
        hosts=[{"host": "localhost", "port": 9200}],
        grpc_hosts=[{"host": "localhost", "port": 9400}],
    )

    # TLS with server verification
    client = OpenSearchGrpc(
        hosts=[{"host": "localhost", "port": 9200}],
        grpc_hosts=[{"host": "localhost", "port": 9400}],
        use_ssl=True,
        ca_certs="/path/to/root-ca.pem",
    )

    # TLS with ssl_context
    import ssl
    ctx = ssl.create_default_context(cafile="/path/to/root-ca.pem")
    client = OpenSearchGrpc(
        hosts=[{"host": "localhost", "port": 9200}],
        grpc_hosts=[{"host": "localhost", "port": 9400}],
        use_ssl=True,
        ssl_context=ctx,
    )

    # Mutual TLS (mTLS)
    client = OpenSearchGrpc(
        hosts=[{"host": "localhost", "port": 9200}],
        grpc_hosts=[{"host": "localhost", "port": 9400}],
        use_ssl=True,
        ca_certs="/path/to/root-ca.pem",
        client_cert="/path/to/client-cert.pem",
        client_key="/path/to/client-key.pem",
    )
"""

import base64
import re
import ssl
from typing import (
    Any,
    Callable,
    Collection,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import grpc
from opensearch.protobufs.services import (
    document_service_pb2_grpc,
    ml_service_pb2_grpc,
)

from opensearch_grpc.ml_translation import (
    MlExecuteAgentStreamRequestBuilder,
    MlPredictModelStreamRequestBuilder,
    MlStreamResponseConverter,
)
from opensearch_grpc.translation import BulkRequestProtoBuilder, ResponseConverter
from opensearchpy.exceptions import (
    AuthenticationException,
    AuthorizationException,
    ConflictError,
    ConnectionError,
    ConnectionTimeout,
    NotFoundError,
    RequestError,
    SSLError,
    TransportError,
)
from opensearchpy.transport import Transport


class BasicAuthInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
    grpc.StreamUnaryClientInterceptor,  # type: ignore[misc]
    grpc.StreamStreamClientInterceptor,  # type: ignore[misc]
):
    """gRPC interceptor that adds Basic auth to every call (unary and streaming).

    Attaches an 'authorization' metadata header with base64-encoded
    credentials, matching how the REST client sends Basic auth.
    """

    def __init__(self, username: str, password: str) -> None:
        credentials = f"{username}:{password}".encode("utf-8")
        self._auth_header = f"Basic {base64.b64encode(credentials).decode('utf-8')}"

    def _add_auth_metadata(self, client_call_details: Any) -> Any:
        metadata = list(client_call_details.metadata or [])
        metadata.append(("authorization", self._auth_header))
        return client_call_details._replace(metadata=metadata)

    def intercept_unary_unary(
        self, continuation: Any, client_call_details: Any, request: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request)

    def intercept_unary_stream(
        self, continuation: Any, client_call_details: Any, request: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request)

    def intercept_stream_unary(
        self, continuation: Any, client_call_details: Any, request_iterator: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request_iterator)

    def intercept_stream_stream(
        self, continuation: Any, client_call_details: Any, request_iterator: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request_iterator)


class BearerTokenInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
    grpc.StreamUnaryClientInterceptor,  # type: ignore[misc]
    grpc.StreamStreamClientInterceptor,  # type: ignore[misc]
):
    """gRPC interceptor that adds Bearer token auth to every call (unary and streaming)."""

    def __init__(self, token: str) -> None:
        if token.lower().startswith("bearer "):
            self._auth_header = token
        else:
            self._auth_header = f"Bearer {token}"

    def _add_auth_metadata(self, client_call_details: Any) -> Any:
        metadata = list(client_call_details.metadata or [])
        metadata.append(("authorization", self._auth_header))
        return client_call_details._replace(metadata=metadata)

    def intercept_unary_unary(
        self, continuation: Any, client_call_details: Any, request: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request)

    def intercept_unary_stream(
        self, continuation: Any, client_call_details: Any, request: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request)

    def intercept_stream_unary(
        self, continuation: Any, client_call_details: Any, request_iterator: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request_iterator)

    def intercept_stream_stream(
        self, continuation: Any, client_call_details: Any, request_iterator: Any
    ) -> Any:
        return continuation(self._add_auth_metadata(client_call_details), request_iterator)


class AWSV4GrpcInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
):
    """gRPC interceptor that signs every call with AWS SigV4.

    Supports unary and server-streaming calls. Client-streaming is not
    supported for SigV4 as the request body must be known at sign time.
    """

    def __init__(self, credentials: Any, region: str, service: str = "es", host: str = "localhost") -> None:
        from opensearchpy.helpers.signer import AWSV4Signer

        self._signer = AWSV4Signer(credentials, region, service)
        self._host = host

    def _sign_and_add_metadata(self, client_call_details: Any, request: Any) -> Any:
        grpc_method = client_call_details.method
        url = f"https://{self._host}{grpc_method}"
        body = request.SerializeToString() if hasattr(request, "SerializeToString") else None
        signed_headers = self._signer.sign(method="POST", url=url, body=body)

        metadata = list(client_call_details.metadata or [])
        for key, value in signed_headers.items():
            metadata.append((key.lower(), value))

        return client_call_details._replace(metadata=metadata)

    def intercept_unary_unary(
        self, continuation: Any, client_call_details: Any, request: Any
    ) -> Any:
        return continuation(self._sign_and_add_metadata(client_call_details, request), request)

    def intercept_unary_stream(
        self, continuation: Any, client_call_details: Any, request: Any
    ) -> Any:
        return continuation(self._sign_and_add_metadata(client_call_details, request), request)


class GrpcTransport(Transport):
    """
    Transport that routes bulk and ML streaming operations over gRPC.

    Bulk requests are sent via DocumentService.Bulk for better performance.
    ML prediction and agent execution use gRPC server-streaming.
    All other operations fall back to REST automatically.

    Channel Security:
        - use_ssl=False (default): grpc.insecure_channel
        - use_ssl=True: grpc.secure_channel with ssl_channel_credentials
        - ca_certs: Root CA for server verification (or system defaults)
        - client_cert + client_key: Mutual TLS (mTLS)

    Error Handling:
        gRPC errors are mapped to opensearch-py exceptions:
        - UNAVAILABLE → ConnectionError (retried)
        - DEADLINE_EXCEEDED → ConnectionTimeout (retried if retry_on_timeout)
        - UNAUTHENTICATED → AuthenticationException
        - PERMISSION_DENIED → AuthorizationException
        - NOT_FOUND → NotFoundError
        - ALREADY_EXISTS → ConflictError
        - INVALID_ARGUMENT → RequestError
        - Other → TransportError

    Retry Behavior:
        ConnectionError and ConnectionTimeout are retried up to max_retries
        times, matching the REST transport behavior. After retries are
        exhausted, the error is raised to the user (no silent REST fallback).
    """

    def __init__(self, hosts: Any, *args: Any, **kwargs: Any) -> None:
        self._grpc_port = kwargs.pop("grpc_port", 9400)
        self._grpc_hosts = kwargs.pop("grpc_hosts", None)

        # Read auth params (don't pop — REST fallback needs them too)
        self._http_auth = kwargs.get("http_auth", None)

        # Read TLS params (don't pop — REST fallback needs them too)
        self._use_ssl = kwargs.get("use_ssl", False)
        self._verify_certs = kwargs.get("verify_certs", True)
        self._ssl_context = kwargs.get("ssl_context", None)
        self._ca_certs = kwargs.get("ca_certs", None)
        self._client_cert = kwargs.get("client_cert", None)
        self._client_key = kwargs.get("client_key", None)
        self._ssl_assert_hostname = kwargs.get("ssl_assert_hostname", None)

        # Validate single gRPC host — multiple targets not yet supported
        if self._grpc_hosts and len(self._grpc_hosts) > 1:
            raise ValueError("Multiple gRPC host targets not yet supported")

        super().__init__(hosts, *args, **kwargs)

        # Resolve gRPC target — grpc_hosts is required
        if not self._grpc_hosts:
            raise ValueError("grpc_hosts parameter is required for GrpcTransport")

        first_grpc = (
            self._grpc_hosts[0]
            if isinstance(self._grpc_hosts[0], dict)
            else {"host": self._grpc_hosts[0]}
        )
        grpc_host = first_grpc.get("host", "localhost")
        grpc_port = first_grpc.get("port", self._grpc_port)

        self._grpc_address = f"{grpc_host}:{grpc_port}"

        # Create channel — secure (TLS/mTLS) or insecure
        # TLS behavior:
        #   - use_ssl=True + ssl_context: Extract CA certs from context
        #   - use_ssl=True + ca_certs: Verify server using provided CA
        #   - use_ssl=True + no ca_certs/ssl_context: Verify using system CAs
        #   - use_ssl=True + client_cert + client_key: Mutual TLS (mTLS)
        #   - use_ssl=False: No encryption (insecure channel)
        if self._use_ssl:
            # gRPC does not support disabling certificate verification.
            # Surface error immediately if verify_certs=False without CA certs.
            if not self._verify_certs and not self._ca_certs and not self._ssl_context:
                raise ValueError(
                    "gRPC does not support verify_certs=False. The gRPC channel "
                    "requires valid certificate verification. "
                    "For self-signed certificates, provide ca_certs or ssl_context."
                )

            # Determine root CA certificates
            root_certs = None
            if self._ssl_context:
                # Extract CA certs from ssl.SSLContext (DER → PEM)
                root_certs = self._extract_ca_certs_from_context(self._ssl_context)
            elif self._ca_certs:
                with open(self._ca_certs, "rb") as f:
                    root_certs = f.read()

            # Load client certificate and key for mutual TLS (mTLS)
            private_key = None
            cert_chain = None
            if self._client_cert:
                with open(self._client_cert, "rb") as f:
                    cert_chain = f.read()
            if self._client_key:
                with open(self._client_key, "rb") as f:
                    private_key = f.read()

            credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certs,
                private_key=private_key,
                certificate_chain=cert_chain,
            )
            # Build channel options
            options = []
            if self._ssl_assert_hostname:
                options.append(
                    ("grpc.ssl_target_name_override", self._ssl_assert_hostname)
                )

            self._channel = grpc.secure_channel(
                self._grpc_address, credentials, options=options or None
            )
        else:
            self._channel = grpc.insecure_channel(self._grpc_address)

        # Wrap channel with auth interceptor if credentials provided
        if self._http_auth is not None:
            if callable(self._http_auth) and not isinstance(self._http_auth, (tuple, list)):
                # Callable auth — SigV4 signer
                if hasattr(self._http_auth, "signer"):
                    interceptor = AWSV4GrpcInterceptor(
                        credentials=self._http_auth.signer.credentials,
                        region=self._http_auth.signer.region,
                        service=self._http_auth.signer.service,
                        host=grpc_host,
                    )
                else:
                    raise NotImplementedError(
                        "Custom callable auth is not supported for gRPC. "
                        "Use AWSV4SignerAuth or http_auth=('user', 'pass')."
                    )
            elif isinstance(self._http_auth, (tuple, list)):
                username, password = self._http_auth[0], self._http_auth[1]
                interceptor = BasicAuthInterceptor(username, password)
            elif isinstance(self._http_auth, str) and (
                self._http_auth.startswith("Bearer ")
                or self._http_auth.startswith("bearer ")
            ):
                interceptor = BearerTokenInterceptor(self._http_auth)
            else:
                # String format "user:pass"
                username, password = str(self._http_auth).split(":", 1)
                interceptor = BasicAuthInterceptor(username, password)
            self._channel = grpc.intercept_channel(self._channel, interceptor)

        self._document_stub = document_service_pb2_grpc.DocumentServiceStub(
            self._channel
        )
        self._ml_stub = ml_service_pb2_grpc.MLServiceStub(self._channel)

    def perform_request(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
        timeout: Optional[Union[int, float]] = None,
        ignore: Collection[int] = (),
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """Route to gRPC or REST based on the URL pattern."""
        handler = self._get_grpc_handler(method, url)
        if handler:
            # Ensure channel is healthy before attempting gRPC
            self._ensure_channel_connected()
            # Retry loop for gRPC — mirrors Transport.perform_request behavior
            for attempt in range(self.max_retries + 1):
                try:
                    return handler(method, url, params, body)
                except ConnectionTimeout:
                    if self.retry_on_timeout and attempt < self.max_retries:
                        continue
                    raise
                except ConnectionError:
                    if attempt < self.max_retries:
                        # Attempt channel reconnect before next retry
                        self._reconnect_channel()
                        continue
                    raise
                except TransportError as e:
                    if (
                        hasattr(e, "status_code")
                        and e.status_code in self.retry_on_status
                        and attempt < self.max_retries
                    ):
                        continue
                    # Non-retryable errors (auth, request errors) should NOT
                    # fall back to REST — raise immediately
                    raise

        return super().perform_request(
            method,
            url,
            params=params,
            body=body,
            timeout=timeout,
            ignore=ignore,
            headers=headers,
        )

    # Matches: /_bulk or /<index>/_bulk
    _BULK_PATTERN = re.compile(r"^/([^/]+/)?_bulk$")

    def _get_grpc_handler(self, method: str, url: str) -> Optional[Callable[..., Any]]:
        """Determine if this request can be handled via gRPC.

        Only bulk requests are routed over gRPC.
        All other operations fall through to REST.

        Matches endpoints:
            POST /_bulk
            POST /<index>/_bulk
        """
        if method in ("POST", "PUT") and self._BULK_PATTERN.match(url):
            return self._handle_bulk

        return None

    # ─── gRPC Handlers ────────────────────────────────────────────────────────

    def _handle_bulk(
        self, method: str, url: str, params: Optional[Mapping[str, Any]], body: Any
    ) -> Any:
        """Bulk → DocumentService.Bulk (native gRPC)."""
        url_index = self._extract_index_from_url(url, "_bulk")
        refresh = params.get("refresh") if params else None
        timeout = params.get("timeout") if params else None
        pipeline = params.get("pipeline") if params else None
        routing = params.get("routing") if params else None

        converter = BulkRequestProtoBuilder.from_body(
            body,
            index=url_index,
            refresh=refresh,
            timeout=timeout,
            pipeline=pipeline,
            routing=routing,
        )

        try:
            response = self._document_stub.Bulk(converter.build())
        except grpc.RpcError as e:
            self._raise_grpc_error(e)

        return ResponseConverter._convert_bulk_items(response)

    # ─── ML gRPC Streaming ──────────────────────────────────

    def predict_model_stream(
        self,
        model_id: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> Iterator[Any]:
        """Predict a model in streaming mode via MLService.PredictModelStream.

        :arg model_id: the deployed model id.
        :arg body: REST-style body, e.g. ``{"parameters": {"messages": [...]}}``.
        """
        request = MlPredictModelStreamRequestBuilder.from_body(
            model_id=model_id,
            body=dict(body) if body else None,
        ).build()
        return self._stream(self._ml_stub.PredictModelStream, request)

    def execute_agent_stream(
        self,
        agent_id: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> Iterator[Any]:
        """Execute an agent in streaming mode via MLService.ExecuteAgentStream.

        :arg agent_id: the agent id.
        :arg body: REST-style body, e.g. ``{"parameters": {"question": "..."}}``.
        """
        request = MlExecuteAgentStreamRequestBuilder.from_body(
            agent_id=agent_id,
            body=dict(body) if body else None,
        ).build()
        return self._stream(self._ml_stub.ExecuteAgentStream, request)

    def _stream(self, rpc: Callable[[Any], Any], request: Any) -> Iterator[Any]:
        """Iterate a server-streaming RPC, converting each chunk to a dict.

        gRPC errors surface while the stream is consumed, so the conversion
        happens inside the generator and ``grpc.RpcError`` is mapped to the same
        opensearch-py exceptions the REST client raises.
        """
        self._ensure_channel_connected()
        try:
            for response in rpc(request):
                yield MlStreamResponseConverter.from_predict_response(response)
        except grpc.RpcError as e:
            self._raise_grpc_error(e)

    def _raise_grpc_error(self, error: grpc.RpcError) -> None:
        """Convert grpc.RpcError to opensearch-py exceptions.

        Maps gRPC status codes to the same exception types that the REST
        client raises, so users' existing except blocks work unchanged.
        """
        code = error.code()
        details = error.details() or "gRPC error"

        if code == grpc.StatusCode.UNAVAILABLE:
            # Detect SSL/TLS-specific failures
            if "SSL" in details or "TLS" in details or "handshake" in details:
                raise SSLError("N/A", details, error)
            raise ConnectionError("N/A", details, error)
        elif code == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise ConnectionTimeout("TIMEOUT", details, error)
        elif code == grpc.StatusCode.UNAUTHENTICATED:
            raise AuthenticationException(401, details, {"error": details})
        elif code == grpc.StatusCode.PERMISSION_DENIED:
            raise AuthorizationException(403, details, {"error": details})
        elif code == grpc.StatusCode.NOT_FOUND:
            raise NotFoundError(404, details, {"error": details})
        elif code == grpc.StatusCode.ALREADY_EXISTS:
            raise ConflictError(409, details, {"error": details})
        elif code == grpc.StatusCode.INVALID_ARGUMENT:
            raise RequestError(400, details, {"error": details})
        else:
            raise TransportError("N/A", f"gRPC {code.name}: {details}", error)

    def _extract_index_from_url(self, url: str, endpoint: str) -> Optional[str]:
        """Extract index from URL like /my-index/_bulk → 'my-index'."""
        parts = url.strip("/").split("/")
        if len(parts) >= 2 and parts[-1] == endpoint:
            return "/".join(parts[:-1])
        return None

    @staticmethod
    def _extract_ca_certs_from_context(ctx: ssl.SSLContext) -> Optional[bytes]:
        """Extract CA certificates from an ssl.SSLContext as PEM bytes.

        Retrieves all loaded CA certs in DER format and converts them to
        PEM for use with grpc.ssl_channel_credentials(root_certificates=...).

        Returns None if no CA certs are loaded in the context.
        """
        der_certs = ctx.get_ca_certs(binary_form=True)
        if not der_certs:
            return None

        import base64

        pem_certs = []
        for der_cert in der_certs:
            b64 = base64.b64encode(der_cert).decode("ascii")
            # Wrap at 64 characters per line (PEM standard)
            lines = [b64[i : i + 64] for i in range(0, len(b64), 64)]
            pem = "-----BEGIN CERTIFICATE-----\n"
            pem += "\n".join(lines)
            pem += "\n-----END CERTIFICATE-----\n"
            pem_certs.append(pem)

        return "".join(pem_certs).encode("ascii")

    def _ensure_channel_connected(self) -> None:
        """Check channel state and reconnect if in SHUTDOWN state.

        gRPC channels handle TRANSIENT_FAILURE internally with backoff,
        but SHUTDOWN is terminal — the channel must be recreated.
        """
        try:
            state = self._channel.get_state(try_to_connect=False)
            if state == grpc.ChannelConnectivity.SHUTDOWN:
                self._reconnect_channel()
        except AttributeError:
            # get_state not available in all grpc versions — skip check
            pass

    def _reconnect_channel(self) -> None:
        """Recreate the gRPC channel and document stub.

        Called when the channel enters an unrecoverable state or after
        a connection failure during retry.
        """
        try:
            self._channel.close()
        except Exception:
            pass
        self._channel = grpc.insecure_channel(self._grpc_address)
        self._document_stub = document_service_pb2_grpc.DocumentServiceStub(
            self._channel
        )
        self._ml_stub = ml_service_pb2_grpc.MLServiceStub(self._channel)

    def close(self) -> None:
        """Close gRPC channel and REST connections."""
        if self._channel:
            self._channel.close()
        super().close()
