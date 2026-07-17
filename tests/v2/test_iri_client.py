"""IRI adapter tests for credential-safe durable failures."""

import traceback

import pytest

from lclstream_api.v2.clients import iri

RAW_TOKEN = "request-only-secret-token"


@pytest.mark.parametrize(
    ("message", "error_type", "expected"),
    [
        (
            f"401 Authorization: Bearer {RAW_TOKEN}",
            iri.IriAuthenticationError,
            "IRI rejected the delegated credential",
        ),
        (
            f"request failed with Authorization: Bearer {RAW_TOKEN}",
            iri.IriOperationError,
            "IRI job status failed",
        ),
    ],
)
def test_translated_error_traceback_never_persists_upstream_secret(
    message: str, error_type: type[Exception], expected: str
) -> None:
    with pytest.raises(error_type) as exc_info:
        try:
            raise RuntimeError(message)
        except RuntimeError as upstream:
            iri._raise_operation_error(upstream, "job status")

    assert str(exc_info.value) == expected
    formatted = "".join(traceback.format_exception(exc_info.value))
    assert RAW_TOKEN not in formatted
