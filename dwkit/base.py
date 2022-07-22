from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal
from zipfile import ZipFile

import anyio
from aiofile import async_open
from aiomultiprocess import Pool
from tqdm import tqdm

from dwkit.utils import async_read_json


class JsonToolKitBase(ABC):
    def __init__(
        self,
        data_root: str,
        output: str,
        temp_dir: str = "./temp",
        target: Literal["라벨링", "원천"] = "라벨링",
        unzip: bool = True,
        num_proc: int | None = None,
    ):
        """
        압축된 json 파일로 구성된 데이터에 대한 베이스 클래스

        Parameters
        ----------
            data_root:str
                데이터가 저장되어 있는 경로
            output:str
                결과물이 저장될 파일 경로
            temp_dir:str="temp"
                unzip == True일 경우, 압축파일을 풀때 사용되는 임시 폴더를 생성할 경로
            target:Literal["라벨링", "원천"]="라벨링"
                사용할 데이터 형식, "라벨링"은 라벨링 데이터, "원천"은 원천 데이터
            unzip:bool=True
                압축파일을 풀어서 사용할지 여부, 미리 압축을 풀어놨다면
                False로 설정하세요. 파이썬으로 압축 푸는건 반디집보다 느립니다.
            num_proc:int|None=None
                멀티프로세싱에 사용할 프로세스 수. None이면 모두 사용
        """

        self.data_root = Path(data_root)
        self.unzip = unzip
        self.num_proc = num_proc
        self.output = Path(output)
        self.output.parent.mkdir(parents=True, exist_ok=True)

        if not self.output.exists():
            with self.output.open("w", encoding="utf-8"):
                # make file for `async_open`
                pass

        if target not in ("라벨링", "원천"):
            raise ValueError("'target'은 '라벨링' 또는 '원천'으로 지정해야 합니다.")
        self.target = target

        self.temp_dir = None
        if unzip:
            Path(temp_dir).mkdir(parents=True, exist_ok=True)
            self.temp_dir = TemporaryDirectory(dir=temp_dir)

    def __del__(self):
        self.close()

    def get_json_paths(self, root: str | Path) -> list[Path]:
        root_ = Path(root)
        return list(root_.rglob("*.json"))

    @abstractmethod
    def get_zipfile_paths(self) -> list[Path]:
        ...

    @staticmethod
    @abstractmethod
    def read_label_data(data: dict[str, Any]) -> list[str]:
        ...

    @staticmethod
    @abstractmethod
    def read_source_data(data: dict[str, Any]) -> list[str]:
        ...

    async def async_read_data(self, file_path: Path):
        async with async_open(file_path, "rb") as file:
            content = await file.read()
        data = await async_read_json(content)

        if self.target == "라벨링":
            return await anyio.to_thread.run_sync(self.read_label_data, data)
        else:
            return await anyio.to_thread.run_sync(self.read_source_data, data)

    def uncompress(self, zipfile_path: str | Path):
        zipfile = Path(zipfile_path)
        with ZipFile(zipfile) as zf:
            zipfile_size = sum(file.file_size for file in zf.infolist())
            pbar = tqdm(
                total=zipfile_size,
                unit="b",
                unit_scale=True,
                desc=f"{zipfile.name} 압축 푸는중...",
            )
            for file in zf.infolist():
                zf.extract(file, self.temp_dir.name)
                pbar.update(file.file_size)

            pbar.close()

    async def delete_file(self, file_path: Path):
        await anyio.to_thread.run_sync(file_path.unlink)

    async def run_with_unzip(self):
        zipfile_paths = self.get_zipfile_paths()
        for zipfile_path in zipfile_paths:
            self.uncompress(zipfile_path)
            json_paths = self.get_json_paths(self.temp_dir.name)
            await self.parse(json_paths)

            async with Pool(self.num_proc) as pool:
                await pool.map(self.delete_file, json_paths)

    async def run_without_unzip(self):
        json_paths = self.get_json_paths(self.data_root)
        await self.parse(json_paths)

    async def parse(self, json_paths: list[Path]):
        pbar = tqdm(total=len(json_paths), desc="JSON 파일 읽는중...")
        async with Pool(self.num_proc) as pool, async_open(
            self.output, "a+", encoding="utf-8"
        ) as output:
            async for result in pool.map(self.async_read_data, json_paths):
                await output.write("\n".join(result) + "\n")
                pbar.update()

        pbar.close()

    def run(self):
        if self.unzip:
            anyio.run(self.run_with_unzip)
        else:
            anyio.run(self.run_without_unzip)

    def close(self):
        if self.unzip:
            self.temp_dir.cleanup()
