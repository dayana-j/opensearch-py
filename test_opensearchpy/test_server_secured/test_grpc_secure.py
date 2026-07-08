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
test_grpc_secure.py — gRPC TLS + Basic Auth Integration Tests

End-to-end tests verifying that the gRPC transport works correctly with
TLS encryption and basic authentication enabled. Requires OpenSearch
running with secure-transport-grpc and security plugin (FGAC).

Skips automatically if secure gRPC is not available.
"""

import os
from typing import Any
from unittest import SkipTest, TestCase

import grpc

from opensearchpy.client import OpenSearchGrpc
from opensearchpy.exceptions import AuthenticationException

GRPC_PORT = int(os.environ.get("OPENSEARCH_GRPC_PORT", "9400"))
GRPC_HOST = os.environ.get("OPENSEARCH_GRPC_HOST", "localhost")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "https://localhost:9200")
OPENSEARCH_PASSWORD = os.environ.get(
    "OPENSEARCH_INITIAL_ADMIN_PASSWORD", "myStrongPassword123!"
)


def _grpc_tls_available() -> bool:
    """Check if secure gRPC is reachable."""
    try:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(f"{GRPC_HOST}:{GRPC_PORT}", credentials)
        grpc.channel_ready_future(channel).result(timeout=3)
        channel.close()
        return True
    except Exception:
        return False


if not _grpc_tls_available():
    raise SkipTest(f"Secure gRPC not available on {GRPC_HOST}:{GRPC_PORT}")


class TestSecureGrpc(TestCase):
    """Test gRPC client with TLS + basic auth."""

    def _get_client(self, **kwargs: Any) -> OpenSearchGrpc:
        """Create a TLS + auth enabled gRPC client."""
        ca_certs = os.environ.get("OPENSEARCH_CA_CERTS", None)
        defaults: dict = {
            "hosts": [OPENSEARCH_URL],
            "grpc_hosts": [{"host": GRPC_HOST, "port": GRPC_PORT}],
            "http_auth": ("admin", OPENSEARCH_PASSWORD),
            "use_ssl": True,
            "verify_certs": False,
        }
        if ca_certs:
            defaults["ca_certs"] = ca_certs
            defaults["verify_certs"] = True
        defaults.update(kwargs)
        return OpenSearchGrpc(**defaults)

    # ─── Bulk over TLS + Auth ─────────────────────────────────────────────

    def test_bulk_over_secure_channel(self) -> None:
        """Bulk request succeeds over TLS with valid credentials."""
        client = self._get_client()
        try:
            resp = client.bulk(
                body=[
                    {"index": {"_index": "test-secure-grpc", "_id": "1"}},
                    {"title": "Secure doc"},
                ],
                refresh=True,
            )
            self.assertFalse(resp["errors"])
            self.assertEqual(len(resp["items"]), 1)
            self.assertEqual(resp["items"][0]["index"]["result"], "created")
        finally:
            client.indices.delete(index="test-secure-grpc", ignore=[404])
            client.close()

    def test_bulk_multiple_docs(self) -> None:
        """Multiple documents indexed over authenticated TLS channel."""
        client = self._get_client()
        try:
            body = []
            for i in range(5):
                body.append({"index": {"_index": "test-secure-multi", "_id": str(i)}})
                body.append({"title": f"Secure doc {i}", "value": i})

            resp = client.bulk(body=body, refresh=True)
            self.assertFalse(resp["errors"])
            self.assertEqual(len(resp["items"]), 5)
        finally:
            client.indices.delete(index="test-secure-multi", ignore=[404])
            client.close()

    def test_multiple_requests_maintain_auth(self) -> None:
        """Multiple sequential bulk requests all carry credentials."""
        client = self._get_client()
        try:
            for i in range(3):
                resp = client.bulk(
                    body=[
                        {"index": {"_index": "test-secure-seq", "_id": str(i)}},
                        {"title": f"Doc {i}"},
                    ],
                    refresh=True,
                )
                self.assertFalse(resp["errors"])
        finally:
            client.indices.delete(index="test-secure-seq", ignore=[404])
            client.close()

    # ─── REST Fallback with TLS + Auth ────────────────────────────────────

    def test_rest_fallback_search(self) -> None:
        """Search (REST fallback) works with same TLS + auth credentials."""
        client = self._get_client()
        try:
            client.bulk(
                body=[
                    {"index": {"_index": "test-secure-search", "_id": "1"}},
                    {"title": "Searchable", "category": "secure"},
                ],
                refresh=True,
            )
            resp = client.search(
                index="test-secure-search",
                body={"query": {"match": {"category": "secure"}}},
            )
            self.assertEqual(resp["hits"]["total"]["value"], 1)
        finally:
            client.indices.delete(index="test-secure-search", ignore=[404])
            client.close()

    def test_rest_fallback_get(self) -> None:
        """GET (REST fallback) works with same TLS + auth credentials."""
        client = self._get_client()
        try:
            client.bulk(
                body=[
                    {"index": {"_index": "test-secure-get", "_id": "1"}},
                    {"title": "Get me securely"},
                ],
                refresh=True,
            )
            doc = client.get(index="test-secure-get", id="1")
            self.assertEqual(doc["_source"]["title"], "Get me securely")
        finally:
            client.indices.delete(index="test-secure-get", ignore=[404])
            client.close()

    # ─── Auth Failure ─────────────────────────────────────────────────────

    def test_invalid_password_raises_authentication_exception(self) -> None:
        """Wrong password raises AuthenticationException."""
        client = self._get_client(http_auth=("admin", "wrongpassword"))
        try:
            with self.assertRaises(AuthenticationException):
                client.bulk(
                    body=[
                        {"index": {"_index": "test-secure-badauth", "_id": "1"}},
                        {"title": "Should fail"},
                    ],
                )
        finally:
            client.close()

    def test_no_credentials_raises_authentication_exception(self) -> None:
        """No credentials on a secured node raises AuthenticationException."""
        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            use_ssl=True,
            verify_certs=False,
        )
        try:
            with self.assertRaises(AuthenticationException):
                client.bulk(
                    body=[
                        {"index": {"_index": "test-secure-noauth", "_id": "1"}},
                        {"title": "Should fail"},
                    ],
                )
        finally:
            client.close()


class TestTlsSettings(TestCase):
    """Test various TLS configuration options."""

    def _bulk_succeeds(self, client: OpenSearchGrpc) -> bool:
        """Helper: attempt a bulk request and return True if it succeeds."""
        try:
            resp = client.bulk(
                body=[
                    {"index": {"_index": "test-tls-settings", "_id": "1"}},
                    {"title": "TLS settings test"},
                ],
                refresh=True,
            )
            return not resp["errors"]
        finally:
            client.indices.delete(index="test-tls-settings", ignore=[404])

    def test_use_ssl_true_with_verify_certs_false(self) -> None:
        """TLS channel with verify_certs=False (skip server cert validation)."""
        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            http_auth=("admin", OPENSEARCH_PASSWORD),
            use_ssl=True,
            verify_certs=False,
        )
        try:
            self.assertTrue(self._bulk_succeeds(client))
        finally:
            client.close()

    def test_use_ssl_true_with_ca_certs(self) -> None:
        """TLS channel with explicit CA cert for server verification."""
        ca_certs = os.environ.get("OPENSEARCH_CA_CERTS", None)
        if not ca_certs:
            self.skipTest("OPENSEARCH_CA_CERTS not set — cannot test ca_certs param")

        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            http_auth=("admin", OPENSEARCH_PASSWORD),
            use_ssl=True,
            ca_certs=ca_certs,
            verify_certs=True,
        )
        try:
            self.assertTrue(self._bulk_succeeds(client))
        finally:
            client.close()

    def test_use_ssl_true_with_ssl_context(self) -> None:
        """TLS channel using ssl_context to provide CA certs."""
        import ssl

        ca_certs = os.environ.get("OPENSEARCH_CA_CERTS", None)
        if not ca_certs:
            # Use a permissive context when CA cert path isn't available
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            ctx = ssl.create_default_context(cafile=ca_certs)

        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            http_auth=("admin", OPENSEARCH_PASSWORD),
            use_ssl=True,
            ssl_context=ctx,
        )
        try:
            self.assertTrue(self._bulk_succeeds(client))
        finally:
            client.close()

    def test_ssl_assert_hostname_override(self) -> None:
        """TLS channel with ssl_assert_hostname overriding expected server name."""
        # ssl_assert_hostname maps to grpc.ssl_target_name_override
        # Using the actual hostname should succeed
        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            http_auth=("admin", OPENSEARCH_PASSWORD),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=GRPC_HOST,
        )
        try:
            self.assertTrue(self._bulk_succeeds(client))
        finally:
            client.close()

    def test_ssl_version_accepted_silently(self) -> None:
        """ssl_version is accepted without error (gRPC auto-negotiates)."""
        import ssl

        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            http_auth=("admin", OPENSEARCH_PASSWORD),
            use_ssl=True,
            verify_certs=False,
            ssl_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        try:
            self.assertTrue(self._bulk_succeeds(client))
        finally:
            client.close()

    def test_use_ssl_false_against_tls_server_fails(self) -> None:
        """Insecure channel against a TLS-enabled server should fail."""
        from opensearchpy.exceptions import ConnectionError

        client = OpenSearchGrpc(
            hosts=[OPENSEARCH_URL],
            grpc_hosts=[{"host": GRPC_HOST, "port": GRPC_PORT}],
            http_auth=("admin", OPENSEARCH_PASSWORD),
            use_ssl=False,
        )
        try:
            with self.assertRaises((ConnectionError, Exception)):
                client.bulk(
                    body=[
                        {"index": {"_index": "test-tls-insecure", "_id": "1"}},
                        {"title": "Should fail"},
                    ],
                )
        finally:
            client.close()
