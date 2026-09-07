"""Request schemas. One model per endpoint: the schema IS the contract, so a body
with unknown fields, a missing half, or an empty delivery fails validation instead of
being interpreted."""
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Item(BaseModel):
    model_config = ConfigDict(extra="forbid")
    feed: str = Field(description="Hierarchical feed name, '/'-separated; trailing slashes are trimmed")
    uri: str = Field(description="The feed's identifier for this content (any URI, not necessarily fetchable)")

    @field_validator("feed")
    @classmethod
    def _feed(cls, v: str) -> str:
        # Trailing slashes are normalized away so the 'feed/' subtree selector in
        # /v1/search can never miss a literal trailing-slash feed - none can exist.
        v = v.strip().rstrip("/")
        if not v:
            raise ValueError("feed must not be empty")
        return v

    @field_validator("uri")
    @classmethod
    def _uri(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("uri must not be empty")
        return v


class _Claim(_Item):
    """A delivery or a ping carries the feed's meta: free-form, what THIS feed says about
    the uri. One key is reserved: `published_at`, an ISO 8601 date or timestamp that search
    can rank by (see /v1/search weight_recency). It is validated here so the database can
    cast it without defensive code."""
    meta: dict[str, Any] = {}

    @field_validator("meta")
    @classmethod
    def _published_at(cls, v: dict[str, Any]) -> dict[str, Any]:
        if "published_at" in v:
            p = v["published_at"]
            try:
                datetime.fromisoformat(p)
            except (TypeError, ValueError):
                raise ValueError("meta.published_at must be an ISO 8601 date or timestamp string")
        return v


class Ping(_Claim):
    """Verification only: 200 when this feed's claim (uri, meta, sha256) is held completely."""
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FileDelivery(_Claim):
    bytes_b64: str
    filename: str = ""


class ChunkIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    markdown: str | None = None
    render_b64: str | None = None

    @model_validator(mode="after")
    def _at_least_one_half(self):
        if self.markdown is None and self.render_b64 is None:
            raise ValueError("a chunk needs markdown, render_b64 or both")
        if self.markdown is not None:
            if not self.markdown.strip():
                raise ValueError("markdown must not be empty when given")
            if "\x00" in self.markdown:
                raise ValueError("markdown must not contain NUL bytes")
        return self


class ChunksDelivery(_Claim):
    chunks: list[ChunkIn] = Field(min_length=1, description="The uri's complete current content, in order")


class Delete(_Item):
    """Withdraws this feed's claim on the uri; orphaned content is swept by /v1/gc."""
