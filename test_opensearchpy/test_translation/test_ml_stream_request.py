# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
"""
test_ml_stream_request.py — Unit Tests for the ML streaming translation layer

Tests the request builders and the streamed-response converter without
requiring a running OpenSearch server. No network calls are made.

Run:
    pytest test_opensearchpy/test_translation/test_ml_stream_request.py -v
"""

from opensearch.protobufs.schemas.common_pb2 import (
    STATUS_COMPLETED,
    STATUS_RUNNING,
    DataAsMap,
    InferenceResults,
    Output,
    PredictResponse,
)

from opensearch_grpc.ml_translation import (
    MlExecuteAgentStreamRequestBuilder,
    MlPredictModelStreamRequestBuilder,
    MlStreamResponseConverter,
    _status_to_str,
)


class TestMlPredictModelStreamRequestBuilder:
    """MlPredictModelStreamRequestBuilder builds correct protobuf requests."""

    def test_model_id_is_set(self) -> None:
        req = MlPredictModelStreamRequestBuilder(model_id="my-model").build()
        assert req.model_id == "my-model"
        assert not req.HasField("ml_predict_model_stream_request_body")

    def test_parameters_messages(self) -> None:
        req = MlPredictModelStreamRequestBuilder(
            model_id="m",
            parameters={"messages": [{"role": "user", "content": "Hello"}]},
        ).build()
        body = req.ml_predict_model_stream_request_body
        assert len(body.parameters.messages) == 1
        assert body.parameters.messages[0].role == "user"
        assert body.parameters.messages[0].content == "Hello"

    def test_parameters_inputs_question_interface(self) -> None:
        req = MlPredictModelStreamRequestBuilder(
            model_id="m",
            parameters={
                "inputs": "the input",
                "question": "a question",
                "x_llm_interface": "bedrock/converse",
            },
        ).build()
        params = req.ml_predict_model_stream_request_body.parameters
        assert params.inputs == "the input"
        assert params.question == "a question"
        assert params.x_llm_interface == "bedrock/converse"

    def test_llm_interface_alias(self) -> None:
        req = MlPredictModelStreamRequestBuilder(
            model_id="m", parameters={"llm_interface": "openai/v1/chat/completions"}
        ).build()
        params = req.ml_predict_model_stream_request_body.parameters
        assert params.x_llm_interface == "openai/v1/chat/completions"

    def test_from_body(self) -> None:
        req = MlPredictModelStreamRequestBuilder.from_body(
            model_id="m", body={"parameters": {"question": "hi"}}
        ).build()
        assert req.model_id == "m"
        assert req.ml_predict_model_stream_request_body.parameters.question == "hi"

    def test_from_body_empty(self) -> None:
        req = MlPredictModelStreamRequestBuilder.from_body(model_id="m").build()
        assert req.model_id == "m"
        assert not req.HasField("ml_predict_model_stream_request_body")


class TestMlExecuteAgentStreamRequestBuilder:
    """MlExecuteAgentStreamRequestBuilder builds correct protobuf requests."""

    def test_agent_id_is_set(self) -> None:
        req = MlExecuteAgentStreamRequestBuilder(agent_id="my-agent").build()
        assert req.agent_id == "my-agent"
        assert not req.HasField("ml_execute_agent_stream_request_body")

    def test_parameters(self) -> None:
        req = MlExecuteAgentStreamRequestBuilder(
            agent_id="a", parameters={"question": "What is OpenSearch?"}
        ).build()
        params = req.ml_execute_agent_stream_request_body.parameters
        assert params.question == "What is OpenSearch?"

    def test_from_body(self) -> None:
        req = MlExecuteAgentStreamRequestBuilder.from_body(
            agent_id="a", body={"parameters": {"inputs": "x"}}
        ).build()
        assert req.agent_id == "a"
        assert req.ml_execute_agent_stream_request_body.parameters.inputs == "x"


class TestMlStreamResponseConverter:
    """MlStreamResponseConverter converts PredictResponse chunks to dicts."""

    def test_data_as_map_chunk(self) -> None:
        response = PredictResponse(
            status=STATUS_RUNNING,
            inference_results=[
                InferenceResults(
                    output=[
                        Output(
                            name="response",
                            data_as_map=DataAsMap(content="Hello", is_last=False),
                        )
                    ]
                )
            ],
        )
        result = MlStreamResponseConverter.from_predict_response(response)
        assert result["status"] == "running"
        output = result["inference_results"][0]["output"][0]
        assert output["name"] == "response"
        assert output["dataAsMap"] == {"content": "Hello", "is_last": False}

    def test_last_chunk(self) -> None:
        response = PredictResponse(
            status=STATUS_COMPLETED,
            inference_results=[
                InferenceResults(
                    output=[Output(data_as_map=DataAsMap(content="", is_last=True))]
                )
            ],
        )
        result = MlStreamResponseConverter.from_predict_response(response)
        assert result["status"] == "completed"
        assert (
            result["inference_results"][0]["output"][0]["dataAsMap"]["is_last"] is True
        )

    def test_result_string_output(self) -> None:
        response = PredictResponse(
            inference_results=[InferenceResults(output=[Output(result="0.42")])]
        )
        result = MlStreamResponseConverter.from_predict_response(response)
        assert result["inference_results"][0]["output"][0]["result"] == "0.42"
        assert "status" not in result

    def test_empty_response(self) -> None:
        result = MlStreamResponseConverter.from_predict_response(PredictResponse())
        assert result == {}


class TestHelpers:
    def test_status_to_str_known(self) -> None:
        assert _status_to_str(STATUS_RUNNING) == "running"
        assert _status_to_str(STATUS_COMPLETED) == "completed"

    def test_status_to_str_unknown(self) -> None:
        assert _status_to_str(999) == "unspecified"
