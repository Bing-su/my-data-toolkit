from __future__ import annotations

from typing import Any

import anyio
import orjson


def read_json(content: bytes) -> dict[str, Any]:
    return orjson.loads(content)


async def async_read_json(content: bytes) -> dict[str, Any]:
    return await anyio.to_thread.run_sync(read_json, content)
