from typing import Optional

from typer import Argument, Option, Typer

from ..utils import TargetData
from .web_data_corpus import WebDataCorpus

app = Typer()


@app.command(
    name="web_data_corpus",
    help="대규모 웹데이터 기반 한국어 말뭉치 데이터를 처리합니다.",
    rich_help_panel="대규모 웹데이터 기반 한국어 말뭉치",
)
def web_data_corpus(
    root: str = Argument(
        ...,
        help="대규모 웹데이터 기반 한국어 말뭉치 파일의 경로",
        show_default=False,
        envvar="WEB_DATA_CORPUS_ROOT",
    ),
    output: str = Option(
        "./data/web_data_corpus.txt",
        "-o",
        "--output",
        help="출력 파일 경로",
        envvar="WEB_DATA_CORPUS_OUTPUT",
    ),
    temp_dir: str = Option(
        "./temp", "-tmp", "--temp-dir", help="압축파일을 담을 임시 폴더가 생성될 경로"
    ),
    target: TargetData = Option(
        TargetData.LABEL, "-tg", "--target", help="대상 데이터의 형태", show_default="라벨링"
    ),
    unzip: bool = Option(
        True,
        "--unzip/--no-unzip",
        "-uz/-nuz",
        help="압축파일을 풀어서 사용할지 여부. 미리 압축을 풀어놨을 경우 --no-unzip 옵션을 사용하세요.",
    ),
    num_proc: Optional[int] = Option(
        None, "-n", "--num-proc", help="사용할 프로세서의 수, 1보다 작거나 None이면 모두 사용합니다."
    ),
):
    if isinstance(num_proc, int) and num_proc <= 0:
        num_proc = None

    corpus = WebDataCorpus(
        data_root=root,
        output=output,
        temp_dir=temp_dir,
        target=target,
        unzip=unzip,
        num_proc=num_proc,
    )
    corpus.run()
    corpus.close()


if __name__ == "__main__":
    app()
