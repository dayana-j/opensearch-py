# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# mypy: ignore-errors

"""
test_ml_stream_transport.py — Unit Tests for ML gRPC Streaming Transport

Tests that GrpcTransport.predict_model_stream / execute_agent_stream open the
server stream, convert each chunk, and map gRPC errors — and that the
OpenSearchGrpc client methods delegate to the transport. The gRPC stub is
mocked, so no running server is needed.
"""

from unittest import TestCase
from unittest.mock import MagicMock, patch

import grpc
from opensearch.protobufs.schemas.common_pb2 import (
    STATUS_RUNNING,
    DataAsMap,
    InferenceResults,
    Output,
    PredictResponse,
)

from opensearch_grpc.grpc_transport import GrpcTransport
from opensearchpy.client.grpc_client import OpenSearchGrpc
from opensearchpy.exceptions import NotFoundError


def _running_chunk(content: str, is_last: bool) -> PredictResponse:
    """Build a data_as_map streaming chunk like the ML server emits."""
    return PredictResponse(
        status=STATUS_RUNNING,
        inference_results=[
            InferenceResults(
                output=[
                    Output(
                        name="response",
                        data_as_map=DataAsMap(content=content, is_last=is_last),
                    )
                ]
            )
        ],
    )


class _StreamRpcError(grpc.RpcError):
    """A grpc.RpcError raised while iterating a server stream."""

    def __init__(self, code, details):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


class TestGrpcStreamingMethods(TestCase):
    """GrpcTransport streaming methods open, convert, and map errors."""

    def _get_transport(self) -> GrpcTransport:
        return GrpcTransport(
            [{"host": "localhost", "port": 9200}],
            grpc_hosts=[{"host": "localhost", "port": 9400}],
        )

    def test_predict_model_stream_yields_converted_chunks(self) -> None:
        """PredictModelStream chunks are converted to opensearch-py dicts."""
        t = self._get_transport()
        t._ml_stub.PredictModelStream = MagicMock(
            return_value=iter(
                [_running_chunk("Hel", False), _running_chunk("lo", True)]
            )
        )

        chunks = list(
            t.predict_model_stream(
                model_id="m",
                body={"parameters": {"messages": [{"role": "user", "content": "Hi"}]}},
            )
        )

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["status"], "running")
        self.assertEqual(
            chunks[0]["inference_results"][0]["output"][0]["dataAsMap"]["content"],
            "Hel",
        )
        self.assertTrue(
            chunks[1]["inference_results"][0]["output"][0]["dataAsMap"]["is_last"]
        )
        # The request carried the model id and parameters through to the stub.
        sent = t._ml_stub.PredictModelStream.call_args.args[0]
        self.assertEqual(sent.model_id, "m")
        t.close()

    def test_execute_agent_stream_yields_converted_chunks(self) -> None:
        """ExecuteAgentStream chunks are converted to opensearch-py dicts."""
        t = self._get_transport()
        t._ml_stub.ExecuteAgentStream = MagicMock(
            return_value=iter([_running_chunk("answer", True)])
        )

        chunks = list(
            t.execute_agent_stream(
                agent_id="a", body={"parameters": {"question": "What is OpenSearch?"}}
            )
        )

        self.assertEqual(len(chunks), 1)
        sent = t._ml_stub.ExecuteAgentStream.call_args.args[0]
        self.assertEqual(sent.agent_id, "a")
        t.close()

    def test_stream_without_body(self) -> None:
        """A None body still opens the stream with just the id."""
        t = self._get_transport()
        t._ml_stub.PredictModelStream = MagicMock(return_value=iter([]))

        chunks = list(t.predict_model_stream(model_id="m"))

        self.assertEqual(chunks, [])
        sent = t._ml_stub.PredictModelStream.call_args.args[0]
        self.assertFalse(sent.HasField("ml_predict_model_stream_request_body"))
        t.close()

    def test_stream_error_maps_to_opensearch_exception(self) -> None:
        """A gRPC error raised mid-stream maps to an opensearch-py exception."""
        t = self._get_transport()

        def _raise(_request):
            raise _StreamRpcError(grpc.StatusCode.NOT_FOUND, "Failed to find model")

        t._ml_stub.PredictModelStream = MagicMock(side_effect=_raise)

        with self.assertRaises(NotFoundError):
            list(t.predict_model_stream(model_id="missing"))
        t.close()


class TestOpenSearchGrpcStreamingDelegation(TestCase):
    """OpenSearchGrpc streaming methods delegate to the transport."""

    def _get_client(self) -> OpenSearchGrpc:
        return OpenSearchGrpc(
            hosts=[{"host": "localhost", "port": 9200}],
            grpc_hosts=[{"host": "localhost", "port": 9400}],
        )

    def test_predict_model_stream_delegates(self) -> None:
        client = self._get_client()
        with patch.object(
            client.transport, "predict_model_stream", return_value=iter([{"ok": 1}])
        ) as mocked:
            result = list(client.predict_model_stream(model_id="m", body={"a": 1}))
        self.assertEqual(result, [{"ok": 1}])
        mocked.assert_called_once_with(model_id="m", body={"a": 1})
        client.transport.close()

    def test_execute_agent_stream_delegates(self) -> None:
        client = self._get_client()
        with patch.object(
            client.transport, "execute_agent_stream", return_value=iter([{"ok": 2}])
        ) as mocked:
            result = list(client.execute_agent_stream(agent_id="a", body=None))
        self.assertEqual(result, [{"ok": 2}])
        mocked.assert_called_once_with(agent_id="a", body=None)
        client.transport.close()
