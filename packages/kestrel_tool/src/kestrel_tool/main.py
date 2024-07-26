import typer
from kestrel_tool import mkdb

app = typer.Typer()
app.command()(mkdb.mkdb)


@app.command()
def test():
    """Temp placeholder until we have more commands"""
    pass


if __name__ == "__main__":
    app()
