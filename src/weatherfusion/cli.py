from __future__ import annotations

import json

import click

from .config import load_settings
from .pipeline import run_pipeline


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--home-lat", type=float, help="Home latitude override")
@click.option("--home-lon", type=float, help="Home longitude override")
@click.option("--work-lat", type=float, help="Work latitude override")
@click.option("--work-lon", type=float, help="Work longitude override")
@click.option("--work-address", type=str, help="Work address override")
@click.option("--days", type=int, help="Forecast horizon days")
@click.option("--primary", type=click.Choice(["PUBLIC_FILES", "RSS"], case_sensitive=False), help="Primary ingest path")
@click.option("--rss-fallback/--no-rss-fallback", default=None, help="Enable RSS fallback")
@click.option("--out", "out_dir", type=click.Path(path_type=str), help="Artifact directory")
@click.option("--logs-dir", type=click.Path(path_type=str), help="Log directory")
@click.option("--user-agent", type=str, help="Custom user agent")
@click.option("--no-cache", is_flag=True, help="Force re-download of data")
@click.option("--html-only", is_flag=True, help="Skip email even if credentials exist")
def main(**kwargs):
    """Run the dual-path EHS forecast pipeline."""
    settings = load_settings(kwargs)
    summary = run_pipeline(settings)
    click.echo(
        json.dumps(
            {
                "html_report": summary.html_report,
                "csv_paths": summary.csv_paths,
                "email_sent": summary.email_sent,
            },
            indent=2,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
