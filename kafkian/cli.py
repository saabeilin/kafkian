from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

from kafkian.avsc_to_pydantic import generate_from_dir

app = typer.Typer(help="Generate Pydantic models from Avro .avsc schema files.")


@app.command()
def generate(
    avsc_dir: Annotated[
        Path,
        typer.Argument(help="Directory containing .avsc files."),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "-o",
            "--output",
            help="Write generated code to this file instead of stdout.",
        ),
    ] = None,
) -> None:
    """Read all .avsc files in AVSC_DIR and emit Pydantic model source code."""
    if not avsc_dir.is_dir():
        typer.echo(f"Error: {avsc_dir} is not a directory.", err=True)
        raise typer.Exit(code=1)

    source = generate_from_dir(avsc_dir)

    if output is None:
        sys.stdout.write(source)
    else:
        output.write_text(source)
        typer.echo(f"Written to {output}")


if __name__ == "__main__":
    app()
