# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""
search_translation.py — Search Request/Response translation for gRPC.

Converts between opensearch-py's REST-style search API and the
SearchService protobuf messages.

Supported query types:
    - match_all
    - match_none

Unsupported queries trigger REST fallback via GrpcTransport.
Additional query types (term, match, bool, range, etc.) can be added
by extending the _QUERY_BUILDERS map in SearchRequestProtoBuilder.
"""

import json
from typing import Any, Callable, Dict, Mapping, Optional

from opensearch.protobufs.schemas import common_pb2


class SearchRequestProtoBuilder:
    """Builds a SearchRequest protobuf from REST-style search parameters.

    Usage::

        builder = SearchRequestProtoBuilder.from_rest(
            index="my-index",
            body={"query": {"match_all": {}}},
            params={"size": 10},
        )
        request = builder.build()
        if request is None:
            # Unsupported query — fall back to REST
            ...
    """

    def __init__(self) -> None:
        self._request = common_pb2.SearchRequest()
        self._unsupported_query = False
        self._unsupported_query_type: Optional[str] = None

    @classmethod
    def from_rest(
        cls,
        index: Optional[str] = None,
        body: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> "SearchRequestProtoBuilder":
        """Create a builder from REST-style search parameters.

        :arg index: Comma-separated index names or None for all.
        :arg body: The search body dict (query, size, from, sort, etc).
        :arg params: URL query parameters (size, routing, scroll, etc).
        """
        builder = cls()

        # Set index
        if index:
            for idx in str(index).split(","):
                idx = idx.strip()
                if idx:
                    builder._request.index.append(idx)

        # Map URL params to proto fields
        if params:
            builder._apply_params(params)

        # Map body to SearchRequestBody
        if body:
            builder._apply_body(dict(body))

        return builder

    def build(self) -> Any:
        """Build and return the SearchRequest protobuf message.

        Returns None if the query is unsupported (signals REST fallback).
        """
        if self._unsupported_query:
            return None
        return self._request

    @property
    def is_supported(self) -> bool:
        """Whether this search can be handled via gRPC."""
        return not self._unsupported_query

    @property
    def unsupported_query_type(self) -> Optional[str]:
        """The query type that is not supported, or None."""
        return self._unsupported_query_type

    def _apply_params(self, params: Mapping[str, Any]) -> None:
        """Map REST URL query params to SearchRequest proto fields."""
        # Boolean params
        _bool_params = {
            "allow_no_indices": "allow_no_indices",
            "allow_partial_search_results": "allow_partial_search_results",
            "analyze_wildcard": "analyze_wildcard",
            "ccs_minimize_roundtrips": "ccs_minimize_roundtrips",
            "ignore_throttled": "ignore_throttled",
            "ignore_unavailable": "ignore_unavailable",
            "phase_took": "phase_took",
            "request_cache": "request_cache",
            "rest_total_hits_as_int": "total_hits_as_int",
            "typed_keys": "typed_keys",
        }
        for rest_key, proto_field in _bool_params.items():
            if rest_key in params and params[rest_key] is not None:
                setattr(
                    self._request,
                    proto_field,
                    str(params[rest_key]).lower() in ("true", "1", "yes"),
                )

        # Integer params
        _int_params = {
            "batched_reduce_size": "batched_reduce_size",
            "max_concurrent_shard_requests": "max_concurrent_shard_requests",
            "pre_filter_shard_size": "pre_filter_shard_size",
            "suggest_size": "suggest_size",
        }
        for rest_key, proto_field in _int_params.items():
            if rest_key in params and params[rest_key] is not None:
                setattr(self._request, proto_field, int(params[rest_key]))

        # String params
        _str_params = {
            "preference": "preference",
            "q": "q",
            "scroll": "scroll",
            "suggest_field": "suggest_field",
            "suggest_text": "suggest_text",
        }
        for rest_key, proto_field in _str_params.items():
            if rest_key in params and params[rest_key] is not None:
                setattr(self._request, proto_field, str(params[rest_key]))

        # Routing is repeated string
        if "routing" in params and params["routing"]:
            for r in str(params["routing"]).split(","):
                self._request.routing.append(r.strip())

    def _apply_body(self, body: Dict[str, Any]) -> None:
        """Map the search body dict to SearchRequestBody proto."""
        srb = common_pb2.SearchRequestBody()

        # size
        if "size" in body:
            srb.size = int(body["size"])

        # from
        if "from" in body:
            srb.from_ = int(body["from"])  # type: ignore[attr-defined]

        # timeout
        if "timeout" in body:
            srb.timeout = str(body["timeout"])

        # terminate_after
        if "terminate_after" in body:
            srb.terminate_after = int(body["terminate_after"])

        # track_scores
        if "track_scores" in body:
            srb.track_scores = bool(body["track_scores"])

        # explain
        if "explain" in body:
            srb.explain = bool(body["explain"])

        # stored_fields
        if "stored_fields" in body:
            fields = body["stored_fields"]
            if isinstance(fields, str):
                fields = [f.strip() for f in fields.split(",")]
            for f in fields:
                srb.stored_fields.append(f)

        # _source handling
        if "_source" in body:
            self._apply_source(srb, body["_source"])

        # query
        if "query" in body:
            query_container = self._build_query(body["query"])
            if query_container is None:
                self._unsupported_query = True
                return
            srb.query.CopyFrom(query_container)

        # Set the body on the request
        self._request.search_request_body.CopyFrom(srb)

    def _apply_source(self, srb: Any, source: Any) -> None:
        """Map _source to SourceConfig on SearchRequestBody."""
        sc = common_pb2.SourceConfig()
        if isinstance(source, bool):
            sc.fetch = source
        elif isinstance(source, dict):
            sf = common_pb2.SourceFilter()
            if "includes" in source:
                includes = source["includes"]
                if isinstance(includes, str):
                    includes = [includes]
                for inc in includes:
                    sf.includes.append(inc)
            if "excludes" in source:
                excludes = source["excludes"]
                if isinstance(excludes, str):
                    excludes = [excludes]
                for exc in excludes:
                    sf.excludes.append(exc)
            sc.filter.CopyFrom(sf)
        elif isinstance(source, list):
            sf = common_pb2.SourceFilter()
            for inc in source:
                sf.includes.append(inc)
            sc.filter.CopyFrom(sf)
        srb.x_source.CopyFrom(sc)

    def _build_query(self, query: Dict[str, Any]) -> Optional[Any]:
        """Convert a REST query dict to a QueryContainer proto.

        Returns None if the query type is unsupported (triggers REST fallback).

        To add support for new query types, add an entry to _QUERY_BUILDERS.
        """
        if not query:
            return common_pb2.QueryContainer()

        # Identify query type (first key in the dict)
        query_type = next(iter(query))
        query_body = query[query_type]

        # Dispatch to the appropriate builder
        builder_fn = _QUERY_BUILDERS.get(query_type)
        if builder_fn is None:
            # Unsupported query type — signal REST fallback
            self._unsupported_query_type = query_type
            return None

        qc = common_pb2.QueryContainer()
        result = builder_fn(query_body, qc)
        if result is None:
            return None
        return qc


# ─── Query Builder Functions ────────────────────────────────────────────────
# Each function takes (body, QueryContainer) and populates the appropriate
# field on the QueryContainer. Returns the qc on success, None if unsupported.
#
# To add a new query type:
#   1. Write a function: def _build_<type>(body, qc) -> Optional[QueryContainer]
#   2. Add it to _QUERY_BUILDERS: {"<type>": _build_<type>}


def _build_match_all(body: Any, qc: Any) -> Any:
    """Build match_all query."""
    maq = common_pb2.MatchAllQuery()
    if isinstance(body, dict):
        if "boost" in body:
            maq.boost = float(body["boost"])
        if "_name" in body:
            maq.x_name = body["_name"]
    qc.match_all.CopyFrom(maq)
    return qc


def _build_match_none(body: Any, qc: Any) -> Any:
    """Build match_none query."""
    mnq = common_pb2.MatchNoneQuery()
    if isinstance(body, dict):
        if "boost" in body:
            mnq.boost = float(body["boost"])
        if "_name" in body:
            mnq.x_name = body["_name"]
    qc.match_none.CopyFrom(mnq)
    return qc


# Registry of supported query builders.
# Add new entries here as query support is implemented.
_QUERY_BUILDERS: Dict[str, Callable[..., Any]] = {
    "match_all": _build_match_all,
    "match_none": _build_match_none,
}


# ─── Response Conversion ────────────────────────────────────────────────────


class SearchResponseConverter:
    """Converts a SearchResponse protobuf to a REST-compatible dict.

    Produces the same structure that opensearch-py expects from the
    REST /_search endpoint, so existing user code works unchanged.
    """

    @staticmethod
    def to_dict(response: Any) -> Dict[str, Any]:
        """Convert SearchResponse proto to REST-style dict.

        :arg response: A SearchResponse protobuf message.
        :returns: Dict matching the REST /_search JSON response format.
        """
        result: Dict[str, Any] = {
            "took": response.took,
            "timed_out": response.timed_out,
        }

        # _shards
        if response.HasField("x_shards"):
            shards = response.x_shards
            result["_shards"] = {
                "total": shards.total,
                "successful": shards.successful,
                "failed": shards.failed,
            }
            if shards.HasField("skipped"):
                result["_shards"]["skipped"] = shards.skipped

        # hits
        if response.HasField("hits"):
            result["hits"] = SearchResponseConverter._convert_hits(response.hits)

        # Optional fields
        if response.HasField("terminated_early"):
            result["terminated_early"] = response.terminated_early
        if response.HasField("num_reduce_phases"):
            result["num_reduce_phases"] = response.num_reduce_phases
        if response.HasField("x_scroll_id"):
            result["_scroll_id"] = response.x_scroll_id
        if response.HasField("pit_id"):
            result["pit_id"] = response.pit_id

        return result

    @staticmethod
    def _convert_hits(hits: Any) -> Dict[str, Any]:
        """Convert HitsMetadata proto to dict."""
        result: Dict[str, Any] = {}

        # total
        if hits.HasField("total"):
            total = hits.total
            # HitsMetadataTotal is a oneof: total_hits or int64
            if total.HasField("total_hits"):
                th = total.total_hits
                relation = "eq"
                if th.relation == common_pb2.TOTAL_HITS_RELATION_GTE:
                    relation = "gte"
                result["total"] = {"value": th.value, "relation": relation}
            elif total.HasField("int64"):
                result["total"] = {"value": total.int64, "relation": "eq"}

        # max_score
        if hits.HasField("max_score"):
            ms = hits.max_score
            if ms.HasField("float"):
                result["max_score"] = ms.float
            else:
                result["max_score"] = None

        # hits array
        result["hits"] = []
        for hit in hits.hits:
            result["hits"].append(SearchResponseConverter._convert_hit(hit))

        return result

    @staticmethod
    def _convert_hit(hit: Any) -> Dict[str, Any]:
        """Convert a single HitsMetadataHitsInner proto to dict."""
        hit_dict: Dict[str, Any] = {}

        if hit.HasField("x_index"):
            hit_dict["_index"] = hit.x_index
        if hit.HasField("x_id"):
            hit_dict["_id"] = hit.x_id
        if hit.HasField("x_score"):
            score = hit.x_score
            if score.HasField("double"):
                hit_dict["_score"] = score.double
            else:
                hit_dict["_score"] = None
        if hit.HasField("x_source"):
            # _source is raw bytes (JSON-encoded document)
            try:
                hit_dict["_source"] = json.loads(hit.x_source)
            except (json.JSONDecodeError, UnicodeDecodeError):
                hit_dict["_source"] = hit.x_source.decode("utf-8", errors="replace")

        # Optional metadata
        if hit.HasField("x_version"):
            hit_dict["_version"] = hit.x_version
        if hit.HasField("x_seq_no"):
            hit_dict["_seq_no"] = hit.x_seq_no
        if hit.HasField("x_primary_term"):
            hit_dict["_primary_term"] = hit.x_primary_term
        if hit.HasField("x_routing"):
            hit_dict["_routing"] = hit.x_routing

        # sort values
        if hit.sort:
            hit_dict["sort"] = [
                SearchResponseConverter._field_value_to_python(sv)
                for sv in hit.sort
            ]

        # highlight
        if hit.highlight:
            hit_dict["highlight"] = {
                field: list(fragments.string_array)
                for field, fragments in hit.highlight.items()
            }

        # fields
        if hit.HasField("fields"):
            hit_dict["fields"] = SearchResponseConverter._object_map_to_dict(
                hit.fields
            )

        return hit_dict

    @staticmethod
    def _field_value_to_python(fv: Any) -> Any:
        """Convert a FieldValue proto to a Python value."""
        if fv.HasField("bool"):
            return fv.bool
        elif fv.HasField("general_number"):
            gn = fv.general_number
            if gn.HasField("int32_value"):
                return gn.int32_value
            elif gn.HasField("int64_value"):
                return gn.int64_value
            elif gn.HasField("float_value"):
                return gn.float_value
            elif gn.HasField("double_value"):
                return gn.double_value
            elif gn.HasField("uint64_value"):
                return gn.uint64_value
        elif fv.HasField("string"):
            return fv.string
        elif fv.HasField("null_value"):
            return None
        return None

    @staticmethod
    def _object_map_to_dict(obj_map: Any) -> Dict[str, Any]:
        """Convert an ObjectMap proto to a Python dict."""
        result: Dict[str, Any] = {}
        for key, value in obj_map.fields.items():
            result[key] = SearchResponseConverter._object_value_to_python(value)
        return result

    @staticmethod
    def _object_value_to_python(value: Any) -> Any:
        """Convert an ObjectMap.Value proto to a Python value."""
        which = value.WhichOneof("value")
        if which == "null_value":
            return None
        elif which == "int32":
            return value.int32
        elif which == "int64":
            return value.int64
        elif which == "float":
            return value.float
        elif which == "double":
            return value.double
        elif which == "string":
            return value.string
        elif which == "bool":
            return value.bool
        elif which == "object_map":
            return SearchResponseConverter._object_map_to_dict(value.object_map)
        elif which == "list_value":
            return [
                SearchResponseConverter._object_value_to_python(v)
                for v in value.list_value.value
            ]
        return None
