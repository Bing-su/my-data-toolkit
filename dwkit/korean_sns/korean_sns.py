from __future__ import annotations

from pathlib import Path
from typing import Any

import ijson
from aiofile import async_open
from tqdm import tqdm

from ..base import JsonToolKitBase


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
        raise NotImplementedError

    @staticmethod
    def read_label_data(data: dict[str, Any]) -> list[str]:
        raise NotImplementedError

    async def async_read_data(self, file_path: Path):
        result = []
        item_num = 0
        pbar = tqdm(leave=False)
        async with async_open(file_path, "rb") as file:
            async for item in ijson.items(file, "data.item.body.item"):
                item_num += 1
                pbar.set_description_str(f"{item_num} lines")
                text = item["utterance"]
                if "#" not in text:
                    result.append(text)
        pbar.close()
        return result
