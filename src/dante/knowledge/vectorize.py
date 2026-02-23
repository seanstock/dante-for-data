"""OpenAI embedding generation via text-embedding-3-large.

Uses the async OpenAI client with tenacity retry for robustness.
Raises a clear error if OPENAI_API_KEY is not configured.
"""

from __future__ import annotations

import os
import sys

from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

MODEL = "text-embedding-3-large"
DIMENSIONS = 3072


def _get_client() -> AsyncOpenAI:
    """Return an AsyncOpenAI client, raising a clear error if no API key."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        if sys.platform == "win32":
            hint = "  set OPENAI_API_KEY=sk-..."
        else:
            hint = "  export OPENAI_API_KEY='sk-...'"
        raise RuntimeError(
            "OPENAI_API_KEY environment variable is not set. "
            "Set it to use embedding generation:\n" + hint
        )
    return AsyncOpenAI(api_key=api_key)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
async def generate_embedding(text: str) -> list[float]:
    """Generate an embedding vector for a single text string.

    Returns a list of floats with *DIMENSIONS* elements.
    """
    client = _get_client()
    response = await client.embeddings.create(
        input=text,
        model=MODEL,
        dimensions=DIMENSIONS,
    )
    return response.data[0].embedding


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
async def generate_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Generate embedding vectors for a batch of text strings.

    The OpenAI API supports batching natively. Returns a list of
    embedding vectors in the same order as the input texts.
    """
    if not texts:
        return []

    client = _get_client()
    response = await client.embeddings.create(
        input=texts,
        model=MODEL,
        dimensions=DIMENSIONS,
    )
    # Response data is sorted by index, but let's be explicit
    sorted_data = sorted(response.data, key=lambda x: x.index)
    return [item.embedding for item in sorted_data]
