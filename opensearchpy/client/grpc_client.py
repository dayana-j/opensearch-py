# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.


# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import TYPE_CHECKING, Any, Iterator, cast

from ..exceptions import ImproperlyConfigured
from . import OpenSearch
from .utils import SKIP_IN_PATH, _make_path

if TYPE_CHECKING:
    from opensearch_grpc.grpc_transport import GrpcTransport


class OpenSearchGrpc(OpenSearch):
    """
    OpenSearch client with gRPC transport for bulk and ML streaming operations.

    Extends the standard OpenSearch client with gRPC channel management.
    Bulk requests are routed over gRPC for better performance; ML prediction
    and agent execution can be streamed over gRPC; all other operations fall
    through to REST automatically.

    Supported parameters:
        - hosts: REST endpoint(s) for fallback operations
        - grpc_hosts: gRPC endpoint (required)
        - use_ssl, ca_certs, client_cert, client_key: TLS/mTLS
        - ssl_context: Custom SSL context for CA certs
        - ssl_version: Accepted (gRPC auto-negotiates)
        - ssl_assert_hostname: Maps to grpc.ssl_target_name_override
        - http_auth: Basic auth (tuple/string), Bearer/JWT, or SigV4 (callable)

    Unsupported parameters (raise NotImplementedError):
        - ssl_assert_fingerprint: No gRPC equivalent
        - ssl_show_warn: No gRPC equivalent

    Usage::

        from opensearchpy import OpenSearchGrpc

        client = OpenSearchGrpc(
            hosts=[{'host': 'localhost', 'port': 9200}],
            grpc_hosts=[{'host': 'localhost', 'port': 9400}],
            http_auth=('admin', 'password'),
            use_ssl=True,
            ca_certs='/path/to/root-ca.pem',
        )

        # Bulk goes over gRPC automatically
        client.bulk(body=[...])

        # ML prediction stream over gRPC
        for chunk in client.predict_model_stream(
            model_id='my-model',
            body={'parameters': {'messages': [{'role': 'user', 'content': 'Hi'}]}},
        ):
            print(chunk)

        # ML agent execution stream over gRPC
        for chunk in client.execute_agent_stream(
            agent_id='my-agent',
            body={'parameters': {'question': 'How many indices are in my cluster?'}},
        ):
            print(chunk)

        # Everything else uses REST
        client.search(index='my-index', body={'query': {'match_all': {}}})

    :arg hosts: list of REST nodes (same as OpenSearch client).
    :arg grpc_hosts: list of gRPC nodes, e.g. [{'host': 'localhost', 'port': 9400}].
    :arg kwargs: all other arguments passed to the OpenSearch client for REST fallback.
    """

    # Parameters that have no gRPC equivalent and raise NotImplementedError
    _UNSUPPORTED_TLS_ARGS = (
        "ssl_assert_fingerprint",
        "ssl_show_warn",
    )

    _UNSUPPORTED_AUTH_ARGS: tuple = ()  # type: ignore[type-arg]

    def __init__(
        self,
        hosts: Any = None,
        grpc_hosts: Any = None,
        **kwargs: Any,
    ) -> None:
        try:
            from opensearch_grpc.grpc_transport import GrpcTransport
        except ImportError as e:
            raise ImproperlyConfigured(
                "gRPC dependencies are not installed. "
                "Install them with: pip install opensearch-py[grpc]"
            ) from e

        # Check for unsupported TLS parameters
        for arg in self._UNSUPPORTED_TLS_ARGS:
            if arg in kwargs and kwargs[arg] is not None and kwargs[arg] is not False:
                raise NotImplementedError(
                    f"The '{arg}' parameter is not supported in the gRPC client. "
                    f"There is no gRPC equivalent for this feature."
                )

        # Check for unsupported auth parameters
        for arg in self._UNSUPPORTED_AUTH_ARGS:
            if arg in kwargs and kwargs[arg] is not None:
                raise NotImplementedError(
                    f"The '{arg}' parameter is not supported in the gRPC client."
                )

        if grpc_hosts is not None:
            kwargs["grpc_hosts"] = grpc_hosts

        super().__init__(hosts, transport_class=GrpcTransport, **kwargs)

    def bulk(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Override to bypass NDJSON serialization for gRPC path.

        The base class serializes body to an NDJSON string, then the gRPC
        transport would have to parse it back into dicts. This override
        passes the raw list directly, eliminating redundant serialization.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        if params is None:
            params = {}
        params.update(kwargs)

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_bulk"),
            params=params,
            headers=headers,
            body=body,
        )

    def predict_model_stream(
        self,
        *,
        model_id: Any,
        body: Any = None,
    ) -> Iterator[Any]:
        """
        Predict a model in streaming mode over gRPC.

        :arg model_id: the deployed model id.
        :arg body: request body, e.g. ``{"parameters": {"messages": [...]}}``.
        """
        transport = cast("GrpcTransport", self.transport)
        return transport.predict_model_stream(model_id=model_id, body=body)

    def execute_agent_stream(
        self,
        *,
        agent_id: Any,
        body: Any = None,
    ) -> Iterator[Any]:
        """
        Execute an agent in streaming mode over gRPC.

        :arg agent_id: the agent id.
        :arg body: request body, e.g. ``{"parameters": {"question": "..."}}``.
        """
        transport = cast("GrpcTransport", self.transport)
        return transport.execute_agent_stream(agent_id=agent_id, body=body)
