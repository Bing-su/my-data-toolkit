from typer import Typer

from .korean_sns.cli import app as korean_sns_app
from .web_data_corpus.cli import app as web_data_corpus_app

app = Typer()
app.registered_commands += web_data_corpus_app.registered_commands
app.registered_commands += korean_sns_app.registered_commands

if __name__ == "__main__":
    app()
