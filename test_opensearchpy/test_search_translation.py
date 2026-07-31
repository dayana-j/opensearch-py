# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""Unit tests for opensearch_grpc/search_translation.py."""

from unittest.mock import MagicMock

import pytest

from opensearch_grpc.search_translation import (
    _QUERY_BUILDERS,
    SearchRequestProtoBuilder,
    SearchResponseConverter,
)


class TestSearchRequestProtoBuilder:
    """Tests for SearchRequestProtoBuilder."""

    def test_match_all_basic(self) -> None:
        """match_all with empty body builds successfully."""
        builder = SearchRequestProtoBuilder.from_rest(
            index="my-index",
            body={"query": {"match_all": {}}},
        )
        assert builder.is_supported
        request = builder.build()
        assert request is not None
        assert "my-index" in request.index
        assert request.search_request_body.query.HasField("match_all")

    def test_match_all_with_boost(self) -> None:
        """match_all with boost sets boost on the proto."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {"boost": 1.5}}},
        )
        request = builder.build()
        assert request is not None
        assert request.search_request_body.query.match_all.boost == pytest.approx(1.5)

    def test_match_none(self) -> None:
        """match_none builds successfully."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_none": {}}},
        )
        assert builder.is_supported
        request = builder.build()
        assert request is not None
        assert request.search_request_body.query.HasField("match_none")

    def test_unsupported_query_returns_none(self) -> None:
        """Unsupported query type returns None and marks unsupported."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"term": {"status": "active"}}},
        )
        assert not builder.is_supported
        assert builder.unsupported_query_type == "term"
        assert builder.build() is None

    def test_empty_body(self) -> None:
        """No body builds a valid request (search all)."""
        builder = SearchRequestProtoBuilder.from_rest(index="test-index")
        assert builder.is_supported
        request = builder.build()
        assert request is not None
        assert "test-index" in request.index

    def test_empty_query(self) -> None:
        """Empty query dict builds a valid request."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {}},
        )
        assert builder.is_supported
        request = builder.build()
        assert request is not None

    def test_multiple_indices(self) -> None:
        """Comma-separated indices are split correctly."""
        builder = SearchRequestProtoBuilder.from_rest(
            index="index-a,index-b,index-c",
            body={"query": {"match_all": {}}},
        )
        request = builder.build()
        assert list(request.index) == ["index-a", "index-b", "index-c"]

    def test_size_and_from(self) -> None:
        """size and from are set on SearchRequestBody."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}, "size": 20, "from": 10},
        )
        request = builder.build()
        assert request.search_request_body.size == 20

    def test_timeout_in_body(self) -> None:
        """timeout string is set on SearchRequestBody."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}, "timeout": "30s"},
        )
        request = builder.build()
        assert request.search_request_body.timeout == "30s"

    def test_source_bool_false(self) -> None:
        """_source: false disables source fetching."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}, "_source": False},
        )
        request = builder.build()
        assert request.search_request_body.HasField("x_source")

    def test_source_includes_list(self) -> None:
        """_source as list sets includes filter."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}, "_source": ["title", "date"]},
        )
        request = builder.build()
        src = request.search_request_body.x_source
        assert "title" in src.filter.includes
        assert "date" in src.filter.includes

    def test_source_includes_excludes_dict(self) -> None:
        """_source as dict with includes/excludes."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={
                "query": {"match_all": {}},
                "_source": {"includes": ["title"], "excludes": ["body"]},
            },
        )
        request = builder.build()
        src = request.search_request_body.x_source
        assert "title" in src.filter.includes
        assert "body" in src.filter.excludes

    def test_params_routing(self) -> None:
        """routing param is set as repeated string."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}},
            params={"routing": "shard1,shard2"},
        )
        request = builder.build()
        assert list(request.routing) == ["shard1", "shard2"]

    def test_params_scroll(self) -> None:
        """scroll param is set on request."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}},
            params={"scroll": "5m"},
        )
        request = builder.build()
        assert request.scroll == "5m"

    def test_params_preference(self) -> None:
        """preference param is set on request."""
        builder = SearchRequestProtoBuilder.from_rest(
            body={"query": {"match_all": {}}},
            params={"preference": "_local"},
        )
        request = builder.build()
        assert request.preference == "_local"

    def test_query_builders_registry(self) -> None:
        """_QUERY_BUILDERS has match_all and match_none."""
        assert "match_all" in _QUERY_BUILDERS
        assert "match_none" in _QUERY_BUILDERS


class TestSearchResponseConverter:
    """Tests for SearchResponseConverter."""

    def _make_response(self) -> MagicMock:
        """Create a mock SearchResponse with common fields."""
        response = MagicMock()
        response.took = 5
        response.timed_out = False
        response.HasField = MagicMock(side_effect=self._has_field_factory(response))

        # _shards
        shards = MagicMock()
        shards.total = 5
        shards.successful = 5
        shards.failed = 0
        shards.HasField = MagicMock(return_value=False)
        response.x_shards = shards

        # hits
        hits = MagicMock()
        hits.HasField = MagicMock(return_value=True)

        # total
        total = MagicMock()
        total.HasField = MagicMock(side_effect=lambda f: f == "total_hits")
        total_hits = MagicMock()
        total_hits.value = 1
        total_hits.relation = 1  # TOTAL_HITS_RELATION_EQ
        total.total_hits = total_hits
        hits.total = total

        # max_score
        max_score = MagicMock()
        max_score.HasField = MagicMock(side_effect=lambda f: f == "float")
        max_score.float = 1.0  # noqa: E501
        hits.max_score = max_score

        # hits array (empty for simplicity)
        hits.hits = []
        response.hits = hits

        return response

    @staticmethod
    def _has_field_factory(response: MagicMock):  # type: ignore[no-untyped-def]
        """Factory for HasField side_effect."""
        fields_present = {"x_shards", "hits"}

        def has_field(field: str) -> bool:
            return field in fields_present

        return has_field

    def test_basic_response(self) -> None:
        """Converts a basic response to dict format."""
        response = self._make_response()
        result = SearchResponseConverter.to_dict(response)

        assert result["took"] == 5
        assert result["timed_out"] is False
        assert result["_shards"]["total"] == 5
        assert result["_shards"]["successful"] == 5
        assert result["_shards"]["failed"] == 0
        assert "hits" in result

    def test_hits_total(self) -> None:
        """Converts hits.total correctly."""
        response = self._make_response()
        result = SearchResponseConverter.to_dict(response)

        assert result["hits"]["total"]["value"] == 1
        assert result["hits"]["total"]["relation"] == "eq"

    def test_hits_max_score(self) -> None:
        """Converts hits.max_score correctly."""
        response = self._make_response()
        result = SearchResponseConverter.to_dict(response)

        assert result["hits"]["max_score"] == 1.0

    def test_empty_hits_array(self) -> None:
        """Empty hits array is preserved."""
        response = self._make_response()
        result = SearchResponseConverter.to_dict(response)

        assert result["hits"]["hits"] == []
