# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""Integration tests for gRPC search against a live OpenSearch instance.

Requires OpenSearch running with gRPC enabled on port 9400.
Set GRPC_HOST/GRPC_PORT env vars if non-default.
"""

import os
import time
import warnings

import pytest

from opensearchpy import OpenSearchGrpc


GRPC_HOST = os.environ.get("GRPC_HOST", "localhost")
GRPC_PORT = int(os.environ.get("GRPC_PORT", "9400"))
REST_HOST = os.environ.get("REST_HOST", GRPC_HOST)
REST_PORT = int(os.environ.get("REST_PORT", "9200"))
TEST_INDEX = "grpc-search-test"


@pytest.fixture(scope="module")
def client():
    """Create an OpenSearchGrpc client for testing."""
    try:
        c = OpenSearchGrpc(
            hosts=[{"host": REST_HOST, "port": REST_PORT}],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
        )
        # Verify connectivity
        info = c.info()
        if not info:
            pytest.skip("Cannot connect to OpenSearch")
    except Exception as e:
        pytest.skip(f"OpenSearch not available: {e}")
    yield c
    c.close()


@pytest.fixture(scope="module")
def indexed_data(client):
    """Set up test index with sample documents."""
    # Clean up if exists
    if client.indices.exists(index=TEST_INDEX):
        client.indices.delete(index=TEST_INDEX)

    # Create index
    client.indices.create(
        index=TEST_INDEX,
        body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "status": {"type": "keyword"},
                    "count": {"type": "integer"},
                }
            },
        },
    )

    # Index documents
    docs = [
        {"title": "First document", "status": "active", "count": 10},
        {"title": "Second document", "status": "active", "count": 20},
        {"title": "Third document", "status": "inactive", "count": 30},
    ]
    for i, doc in enumerate(docs):
        client.index(index=TEST_INDEX, id=str(i + 1), body=doc, refresh=True)

    # Wait for refresh
    time.sleep(1)
    yield
    # Cleanup
    client.indices.delete(index=TEST_INDEX, ignore=[404])


class TestGrpcSearchMatchAll:
    """Test match_all query over gRPC."""

    def test_match_all_returns_all_docs(self, client, indexed_data):
        """match_all query returns all documents."""
        result = client.search(
            index=TEST_INDEX,
            body={"query": {"match_all": {}}},
        )
        assert result["hits"]["total"]["value"] == 3
        assert len(result["hits"]["hits"]) == 3

    def test_match_all_has_standard_fields(self, client, indexed_data):
        """Response has took, timed_out, _shards, hits."""
        result = client.search(
            index=TEST_INDEX,
            body={"query": {"match_all": {}}},
        )
        assert "took" in result
        assert "timed_out" in result
        assert "_shards" in result
        assert "hits" in result

    def test_match_all_hit_has_source(self, client, indexed_data):
        """Each hit has _index, _id, _score, _source."""
        result = client.search(
            index=TEST_INDEX,
            body={"query": {"match_all": {}}},
        )
        hit = result["hits"]["hits"][0]
        assert "_index" in hit
        assert "_id" in hit
        assert "_source" in hit
        assert hit["_index"] == TEST_INDEX

    def test_match_all_with_size(self, client, indexed_data):
        """size limits the number of returned hits."""
        result = client.search(
            index=TEST_INDEX,
            body={"query": {"match_all": {}}, "size": 1},
        )
        assert len(result["hits"]["hits"]) == 1
        assert result["hits"]["total"]["value"] == 3

    def test_match_all_without_index(self, client, indexed_data):
        """match_all without index searches all indices."""
        result = client.search(
            body={"query": {"match_all": {}}},
        )
        # Should return at least our 3 docs
        assert result["hits"]["total"]["value"] >= 3


class TestGrpcSearchMatchNone:
    """Test match_none query over gRPC."""

    def test_match_none_returns_no_docs(self, client, indexed_data):
        """match_none query returns zero hits."""
        result = client.search(
            index=TEST_INDEX,
            body={"query": {"match_none": {}}},
        )
        assert result["hits"]["total"]["value"] == 0
        assert len(result["hits"]["hits"]) == 0


class TestGrpcSearchFallback:
    """Test that unsupported queries fall back to REST with a warning."""

    def test_unsupported_query_falls_back_to_rest(self, client, indexed_data):
        """Unsupported query type emits warning and still returns results via REST."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = client.search(
                index=TEST_INDEX,
                body={"query": {"term": {"status": "active"}}},
            )
            # Should still get results via REST fallback
            assert result["hits"]["total"]["value"] == 2

            # Should have emitted a warning
            grpc_warnings = [
                x for x in w if "gRPC search does not yet support" in str(x.message)
            ]
            assert len(grpc_warnings) == 1
            assert "term" in str(grpc_warnings[0].message)
