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
test_ml_stream.py — gRPC ML Streaming Integration Tests

Tests ML prediction and agent execution streamed over gRPC via
MLService.PredictModelStream / ExecuteAgentStream.

These require a deployed remote model / registered agent, so the streaming
assertions skip unless OPENSEARCH_ML_MODEL_ID / OPENSEARCH_ML_AGENT_ID are set.
The wiring tests always run (they only need the gRPC server to be reachable).
"""

import os
from unittest import SkipTest

from . import OpenSearchGrpcTestCase

ML_MODEL_ID = os.environ.get("OPENSEARCH_ML_MODEL_ID")
ML_AGENT_ID = os.environ.get("OPENSEARCH_ML_AGENT_ID")


class TestMLStreamWiring(OpenSearchGrpcTestCase):
    """The streaming methods are exposed on the gRPC client."""

    def test_predict_model_stream_exists(self) -> None:
        self.assertTrue(callable(self.client.predict_model_stream))

    def test_execute_agent_stream_exists(self) -> None:
        self.assertTrue(callable(self.client.execute_agent_stream))


class TestPredictModelStream(OpenSearchGrpcTestCase):
    def test_predict_model_stream(self) -> None:
        """Stream predictions from a deployed model over gRPC."""
        if not ML_MODEL_ID:
            raise SkipTest("OPENSEARCH_ML_MODEL_ID not set")

        chunks = list(
            self.client.predict_model_stream(
                model_id=ML_MODEL_ID,
                body={
                    "parameters": {"messages": [{"role": "user", "content": "Hello"}]}
                },
            )
        )

        self.assertGreater(len(chunks), 0)
        # Each chunk is a dict; the stream ends with is_last=True on the last
        # data_as_map chunk (when the model streams via data_as_map).
        for chunk in chunks:
            self.assertIsInstance(chunk, dict)


class TestExecuteAgentStream(OpenSearchGrpcTestCase):
    def test_execute_agent_stream(self) -> None:
        """Stream agent execution results over gRPC."""
        if not ML_AGENT_ID:
            raise SkipTest("OPENSEARCH_ML_AGENT_ID not set")

        chunks = list(
            self.client.execute_agent_stream(
                agent_id=ML_AGENT_ID,
                body={"parameters": {"question": "What is OpenSearch?"}},
            )
        )

        self.assertGreater(len(chunks), 0)
        for chunk in chunks:
            self.assertIsInstance(chunk, dict)
