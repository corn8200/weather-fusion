from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, Mapping

from ..config import AppSettings

LOGGER = logging.getLogger(__name__)


class EmailClient:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings

    def send(self, subject: str, html_body: str, attachments: Mapping[str, Path]) -> bool:
        email_cfg = self.settings.email
        if not email_cfg.enabled:
            LOGGER.info("Email disabled; skipping send")
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = email_cfg.sender
        msg["To"] = email_cfg.recipient
        msg.set_content("This email requires an HTML-capable client.")
        msg.add_alternative(html_body, subtype="html")

        for label, path in attachments.items():
            with path.open("rb") as fh:
                data = fh.read()
            msg.add_attachment(
                data,
                maintype="text",
                subtype="csv" if path.suffix == ".csv" else "html",
                filename=path.name,
            )

        with smtplib.SMTP(email_cfg.host, email_cfg.port) as smtp:
            smtp.starttls()
            smtp.login(email_cfg.username, email_cfg.password)
            smtp.send_message(msg)
        LOGGER.info("Email delivered to %s", email_cfg.recipient)
        return True
