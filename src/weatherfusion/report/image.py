from __future__ import annotations

from pathlib import Path

from weasyprint import CSS, HTML  # type: ignore[import]

DEFAULT_WIDTH = 960


def render_png(html: str, output_path: Path, width_px: int = DEFAULT_WIDTH) -> None:
    """Render the HTML forecast into a PNG tuned for a half-slide slot."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    css = CSS(
        string=f"""
        @page {{
            size: {width_px}px auto;
            margin: 0;
        }}
        html, body {{
            width: {width_px}px;
            margin: 0;
            padding: 0;
            background: transparent;
        }}
        """
    )
    HTML(string=html).write_png(str(output_path), stylesheets=[css])
