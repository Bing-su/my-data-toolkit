from __future__ import annotations

from pathlib import Path as _Path
from tempfile import TemporaryDirectory
from typing import Any, Literal
from zipfile import ZipFile

import anyio
from aiomultiprocess import Pool
from anyio import Path
from tqdm import tqdm
from tqdm.asyncio import atqdm

from dwkit.utils import async_read_json


class WebDataCorpus:
    def __init__(
        self,
        data_root: str,
        output: str = "data/web_data_corpus.txt",
        temp_dir: str = "temp",
        target: Literal["라벨링", "원천"] = "라벨링",
        unzip: bool = True,
        num_proc: int | None = None,
    ):
        """
        대규모 웹데이터 기반 한국어 말뭉치
        https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&aihubDataSe=realm&dataSetSn=624

        다운로드 받은 대규모 웹데이터 기반 한국어 말뭉치를 정리하기 위한 클래스
        Parameters
        ----------
            data_root:str
                데이터가 저장되어 있는 경로
            output:str="data/web_data_corpus.txt"
                결과물이 저장될 파일 경로
            temp_dir:str="temp"
                unzip == True일 경우, 압축파일을 풀때 사용되는 임시 폴더
            target:Literal["라벨링", "원천"]="라벨링"
                사용할 데이터 형식, "라벨링"은 라벨링 데이터, "원천"은 원천 데이터
            unzip:bool=True
                압축파일을 풀어서 사용할지 여부, 미리 압축을 풀어놨다면
                False로 설정하세요. 파이썬으로 압축 푸는건 반디집보다 느립니다.
            num_proc:int|None=None
                멀티프로세싱에 사용할 프로세스 수. None이면 모두 사용
        """

        self.data_root = Path(data_root)
        self.output = Path(output)
        if target in ("라벨링", "원천"):
            self.target = target
        else:
            raise ValueError("'target'은 '라벨링' 또는 '원천'으로 지정해야 합니다.")
        self.unzip = unzip
        self.num_proc = num_proc
        if unzip:
            _Path(temp_dir).mkdir(parents=True, exist_ok=True)
            self.temp_dir = TemporaryDirectory(dir=temp_dir)
        _Path(output).parent.mkdir(parents=True, exist_ok=True)

    def __del__(self):
        self.temp_dir.cleanup()

    async def get_zipfile_paths(self):
        if self.target == "라벨링":
            pattern = "[TV]L1.zip"
        else:
            pattern = "[TV]S1.zip"

        return self.data_root.rglob(pattern)

    async def get_json_paths(self, root: str | Path):
        root = Path(root)
        return root.rglob("*.json")

    @staticmethod
    def read_label_data(data: dict[str, Any]) -> list[str]:
        result = []
        data = data["named_entity"]
        for subdata in data:
            title = subdata["title"][0]["sentence"]
            if "(이름)" not in title:
                result.append(title)

            for content in subdata["content"]:
                sentence = content["sentence"]
                if "(이름)" not in sentence:
                    result.append(sentence)
        return result

    @staticmethod
    def read_source_data(data: dict[str, Any]) -> list[str]:
        result = []
        data = data["SJML"]["text"]
        for subdata in data:
            title = subdata["title"]
            if "(이름)" not in title:
                result.append(title)

            content = subdata["content"]
            if "(이름)" not in content:
                result.append(content)
        return result

    async def async_read_data(self, file_path: Path):
        content = await file_path.read_bytes()
        data = await async_read_json(content)
        if self.target == "라벨링":
            return anyio.to_thread.run_sync(self.read_label_data, data)
        else:
            return anyio.to_thread.run_sync(self.read_source_data, data)

    def uncompress(self, zipfile_path: str | Path):
        zipfile = _Path(zipfile_path)
        with ZipFile(zipfile) as zf:
            zipfile_size = sum(file.file_size for file in zf.infolist())
            pbar = tqdm(
                total=zipfile_size,
                unit="B",
                unit_scale=True,
                desc=f"{zipfile.name} 압축 푸는중...",
            )
            for file in zf.infolist():
                zf.extract(file, self.temp_dir.name)
                pbar.update(file.file_size)

            pbar.close()

    async def delete_file(self, file_path: Path):
        await file_path.unlink()

    async def run_with_unzip(self):
        zipfile_paths = list(await self.get_zipfile_paths())
        for zipfile_path in zipfile_paths:
            self.uncompress(zipfile_path)

            json_paths = list(await self.get_json_paths(self.temp_dir.name))

            pbar = atqdm(total=len(json_paths), desc="JSON 파일 읽는중...")
            async with Pool(self.num_proc) as pool:
                async for result in pool.map(self.async_read_data, json_paths):
                    await self.output.write_text("\n".join(result) + "\n")
                    await pbar.update()

            await pbar.close()

            async with Pool(self.num_proc) as pool:
                await pool.map(self.delete_file, json_paths)

    async def run_without_unzip(self):
        json_paths = list(await self.get_json_paths(self.data_root))

        pbar = atqdm(total=len(json_paths), desc="JSON 파일 읽는중...")
        async with Pool(self.num_proc) as pool:
            async for result in pool.map(self.async_read_data, json_paths):
                await self.output.write_text("\n".join(result) + "\n")
                await pbar.update()

        await pbar.close()

    def run(self):
        if self.unzip:
            anyio.run(self.run_with_unzip)
        else:
            anyio.run(self.run_without_unzip)

    def close(self):
        self.temp_dir.cleanup()
