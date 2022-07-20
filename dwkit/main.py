from typer import Typer

import web_data_corpus

app = Typer()
app.add_typer(web_data_corpus.app, name="web_data_corpus")

if __name__ == "__main__":
    app()
