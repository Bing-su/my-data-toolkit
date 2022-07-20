from __future__ import annotations

from pathlib import Path
from typing import Any

import anyio
from aiofile import async_open

from ..base import JsonToolKitBase
from ..utils import async_read_json


class KoreanSNS(JsonToolKitBase):
    def __init__(
        self,
        data_root: str,
        output: str,
        temp_dir: str = "./temp",
        unzip: bool = True,
        num_proc: int | None = None,
    ):
        super().__init__(data_root, output, temp_dir, "라벨링", unzip, num_proc)

    def get_zipfile_paths(self) -> list[Path]:
        return list(self.data_root.rglob("*.zip"))

    @staticmethod
    def read_source_data(data: dict[str, Any]) -> list[str]:
        raise NotImplementedError("한국어 SNS 데이터는 원천 데이터가 없습니다.")

    @staticmethod
    def read_label_data(data: dict[str, Any]) -> list[str]:
        result = []
        data = data["data"]
        for subdata in data:
            for body in subdata["body"]:
                text = body["utterance"]
                if "#" not in text:
                    result.append(text)
        return result

    async def async_read_data(self, file_path: Path):
        async with async_open(file_path, "rb") as file:
            content = await file.read()
        data = await async_read_json(content)
        return await anyio.to_thread.run_sync(self.read_source_data, data)
