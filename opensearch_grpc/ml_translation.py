# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""
ml_translation.py — gRPC Translation Layer for ML streaming APIs

MlPredictModelStreamRequestBuilder  — Python dict → MlPredictModelStreamRequest
MlExecuteAgentStreamRequestBuilder  — Python dict → MlExecuteAgentStreamRequest
MlStreamResponseConverter           — Protobuf PredictResponse → Python dict

This module only converts individual messages; opening and iterating the
server stream is the transport layer's responsibility.
"""

from typing import Any, Dict, Optional

from opensearch.protobufs.schemas.common_pb2 import (
    Messages,
    MlExecuteAgentStreamRequest,
    MLExecuteAgentStreamRequestBody,
    MlPredictModelStreamRequest,
    MLPredictModelStreamRequestBody,
    Parameters,
    PredictResponse,
)

# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST BUILDERS — Python client dict → Protobuf request
# ═══════════════════════════════════════════════════════════════════════════════


def _build_parameters(parameters: Dict[str, Any]) -> Parameters:
    """Build a Parameters protobuf from a Python dict.

    Supported keys (all optional):
        messages: list of {"role": str, "content": str}
        inputs: str
        question: str
        x_llm_interface / llm_interface: str
    """
    params = Parameters()
    messages = parameters.get("messages")
    if messages:
        for msg in messages:
            m = Messages()
            if msg.get("role") is not None:
                m.role = msg["role"]
            if msg.get("content") is not None:
                m.content = msg["content"]
            params.messages.append(m)
    if parameters.get("inputs") is not None:
        params.inputs = parameters["inputs"]
    if parameters.get("question") is not None:
        params.question = parameters["question"]
    llm_interface = parameters.get("x_llm_interface", parameters.get("llm_interface"))
    if llm_interface is not None:
        params.x_llm_interface = llm_interface
    return params


class MlPredictModelStreamRequestBuilder:
    """Converts a Python dict into a protobuf MlPredictModelStreamRequest.

    Usage:
        req = MlPredictModelStreamRequestBuilder(
            model_id="my-model",
            parameters={"messages": [{"role": "user", "content": "Hello"}]},
        ).build()
    """

    def __init__(
        self,
        model_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._model_id = model_id
        self._parameters = parameters

    def build(self) -> MlPredictModelStreamRequest:
        """Build the protobuf MlPredictModelStreamRequest."""
        request = MlPredictModelStreamRequest()
        request.model_id = self._model_id

        if self._parameters is not None:
            body = MLPredictModelStreamRequestBody()
            body.parameters.CopyFrom(_build_parameters(self._parameters))
            request.ml_predict_model_stream_request_body.CopyFrom(body)

        return request

    @classmethod
    def from_body(
        cls,
        model_id: str,
        body: Optional[Dict[str, Any]] = None,
    ) -> "MlPredictModelStreamRequestBuilder":
        """Create a builder from a REST-style body ``{"parameters": {...}}``.

        Mirrors: ``client.predict(model_id=..., body={"parameters": {...}})``
        """
        body = body or {}
        return cls(
            model_id=model_id,
            parameters=body.get("parameters"),
        )


class MlExecuteAgentStreamRequestBuilder:
    """Converts a Python dict into a protobuf MlExecuteAgentStreamRequest.

    Usage:
        req = MlExecuteAgentStreamRequestBuilder(
            agent_id="my-agent",
            parameters={"question": "What is OpenSearch?"},
        ).build()
    """

    def __init__(
        self,
        agent_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._agent_id = agent_id
        self._parameters = parameters

    def build(self) -> MlExecuteAgentStreamRequest:
        """Build the protobuf MlExecuteAgentStreamRequest."""
        request = MlExecuteAgentStreamRequest()
        request.agent_id = self._agent_id

        if self._parameters is not None:
            body = MLExecuteAgentStreamRequestBody()
            body.parameters.CopyFrom(_build_parameters(self._parameters))
            request.ml_execute_agent_stream_request_body.CopyFrom(body)

        return request

    @classmethod
    def from_body(
        cls,
        agent_id: str,
        body: Optional[Dict[str, Any]] = None,
    ) -> "MlExecuteAgentStreamRequestBuilder":
        """Create a builder from a REST-style body ``{"parameters": {...}}``.

        Mirrors: ``client.agents.execute(agent_id=..., body={"parameters": {...}})``
        """
        body = body or {}
        return cls(
            agent_id=agent_id,
            parameters=body.get("parameters"),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE CONVERTER — Protobuf PredictResponse → Python client dict
# ═══════════════════════════════════════════════════════════════════════════════


class MlStreamResponseConverter:
    """Converts a streamed protobuf ``PredictResponse`` chunk to a Python dict.

    The server streams ``PredictResponse`` messages; each is converted back to
    the ``inference_results`` structure the REST ML predict API returns:

        {
            "status": "running",
            "inference_results": [
                {"output": [{"name": "response",
                             "dataAsMap": {"content": "...", "is_last": False}}]}
            ]
        }
    """

    @staticmethod
    def from_predict_response(response: PredictResponse) -> Dict[str, Any]:
        """Convert one protobuf ``PredictResponse`` → opensearch-py dict."""
        result: Dict[str, Any] = {}

        if response.HasField("status"):
            result["status"] = _status_to_str(response.status)

        if response.inference_results:
            inference_results = []
            for ir in response.inference_results:
                outputs = [
                    MlStreamResponseConverter._convert_output(o) for o in ir.output
                ]
                inference_results.append({"output": outputs})
            result["inference_results"] = inference_results

        return result

    @staticmethod
    def _convert_output(output: Any) -> Dict[str, Any]:
        """Convert a single ``Output`` message to a dict."""
        item: Dict[str, Any] = {}
        if output.HasField("name"):
            item["name"] = output.name
        if output.HasField("result"):
            item["result"] = output.result
        if output.HasField("data_as_map"):
            data_as_map: Dict[str, Any] = {}
            if output.data_as_map.HasField("content"):
                data_as_map["content"] = output.data_as_map.content
            if output.data_as_map.HasField("is_last"):
                data_as_map["is_last"] = output.data_as_map.is_last
            item["dataAsMap"] = data_as_map
        return item


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _status_to_str(status: int) -> str:
    """Map the ``Status`` enum to a lowercase REST-style string."""
    mapping = {
        0: "unspecified",
        1: "cancelled",
        2: "completed",
        3: "completed_with_error",
        4: "created",
        5: "failed",
        6: "running",
    }
    return mapping.get(status, "unspecified")
