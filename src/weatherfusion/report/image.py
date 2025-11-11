from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pypdfium2 as pdfium
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
    pdf_bytes = HTML(string=html).write_pdf(stylesheets=[css])
    pdf = pdfium.PdfDocument(BytesIO(pdf_bytes))
    try:
        page = pdf[0]
        try:
            width_pts = page.get_width() or width_px
            scale = width_px / width_pts if width_pts else 1.0
            bitmap = page.render(scale=scale)
            try:
                image = bitmap.to_pil()
                image.save(output_path, format="PNG")
            finally:
                bitmap.close()
        finally:
            page.close()
    finally:
        pdf.close()
