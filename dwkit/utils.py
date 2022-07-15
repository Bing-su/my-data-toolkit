from __future__ import annotations

from pathlib import Path
from typing import Any

import aiofiles
import anyio
import orjson


def read_json(file: bytes) -> dict[str, Any]:
    return orjson.loads(file)


async def async_read_json(file_path: str | Path) -> dict[str, Any]:
    async with aiofiles.open(file_path, "rb") as file:
        data = await file.read()
        return await anyio.to_thread.run_sync(read_json, data)
