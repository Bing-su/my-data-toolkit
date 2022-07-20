from __future__ import annotations

from typing import Any

from ..base import JsonToolKitBase


class WebDataCorpus(JsonToolKitBase):
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
            unzip == True일 경우, 압축파일을 풀때 사용되는 임시 폴더를 생성할 경로
        target:Literal["라벨링", "원천"]="라벨링"
            사용할 데이터 형식, "라벨링"은 라벨링 데이터, "원천"은 원천 데이터
        unzip:bool=True
            압축파일을 풀어서 사용할지 여부, 미리 압축을 풀어놨다면
            False로 설정하세요. 파이썬으로 압축 푸는건 반디집보다 느립니다.
        num_proc:int|None=None
            멀티프로세싱에 사용할 프로세스 수. None이면 모두 사용
    """

    def get_zipfile_paths(self):
        if self.target == "라벨링":
            pattern = "[TV]L1.zip"
        else:
            pattern = "[TV]S1.zip"

        return list(self.data_root.rglob(pattern))

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
