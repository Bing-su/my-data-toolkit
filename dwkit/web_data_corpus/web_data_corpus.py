"""
대규모 웹데이터 기반 한국어 말뭉치
https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&aihubDataSe=realm&dataSetSn=624
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Literal, TextIO
from zipfile import ZipFile

import aiofiles
import anyio
import orjson
from aiomultiprocess import Pool
from tqdm import tqdm


def read_json(file: bytes) -> dict[str, Any]:
    return orjson.loads(file)


async def async_read_json(file_path: str | Path) -> dict[str, Any]:
    async with aiofiles.open(file_path, "rb") as file:
        data = await file.read()
        return await anyio.to_thread.run_sync(read_json, data)


async def read_label_data(file_path: str | Path) -> list[str]:
    result = []
    data = await async_read_json(file_path)
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


async def read_origin_data(file_path: str | Path) -> list[str]:
    result = []
    data = await async_read_json(file_path)
    data = data["SJML"]["text"]
    for subdata in data:
        title = subdata["title"]
        if "(이름)" not in title:
            result.append(title)

        content = subdata["content"]
        if "(이름)" not in content:
            result.append(content)
    return result


def get_zipfile_names(
    root: str | Path, target: Literal["라벨링", "원천"] = "라벨링"
) -> list[Path]:
    root = Path(root)
    if target == "라벨링":
        pattern = "[TV]L1.zip"
    elif target == "원천":
        pattern = "[TV]S1.zip"
    else:
        raise ValueError("'target'은 '라벨링' 또는 '원천'으로 지정해야 합니다.")

    return list(root.rglob(pattern))


async def get_all_data(
    root: str | Path,
    output: TextIO,
    target: Literal["라벨링", "원천"] = "라벨링",
):
    zipfiles = get_zipfile_names(root, target)

    func = read_label_data if target == "라벨링" else read_origin_data

    for zipfile in zipfiles:
        with tempfile.TemporaryDirectory() as tmpdir, ZipFile(zipfile) as zf:
            zipfile_size = sum(file.file_size for file in zf.infolist())
            pbar1 = tqdm(
                total=zipfile_size,
                unit="B",
                unit_scale=True,
                desc=f"{zipfile.name} 압축 푸는중...",
            )
            for file in zf.infolist():
                zf.extract(file, tmpdir)
                pbar1.update(file.file_size)

            json_files = list(Path(tmpdir).rglob("*.json"))

            pbar2 = tqdm(total=len(json_files), desc="json파일 읽는중...")
            async with Pool() as pool:
                async for result in pool.map(func, json_files):
                    output.write("\n".join(result) + "\n")
                    pbar2.update()

            pbar1.close()
            pbar2.close()


def web_data_corpus(
    root: str | Path,
    output: str | Path = "data/web_data_corpus.txt",
    target: Literal["라벨링", "원천"] = "라벨링",
):
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    out_file = output.open("w", encoding="utf-8")

    anyio.run(get_all_data, root, out_file, target)
    out_file.close()
