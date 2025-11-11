from __future__ import annotations

from datetime import datetime
from typing import List

from ..config import SiteSettings
from ..models import AlertSummary

ALERTS_URL = "https://api.weather.gov/alerts/active"


class AlertsClient:
    def __init__(self, session) -> None:
        self.session = session

    def fetch(self, site: SiteSettings) -> List[AlertSummary]:
        params = {"point": f"{site.latitude:.4f},{site.longitude:.4f}"}
        resp = self.session.get(ALERTS_URL, params=params, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        payload = resp.json()
        alerts: List[AlertSummary] = []
        for feature in payload.get("features", []):
            props = feature.get("properties", {})
            event = props.get("event")
            severity = props.get("severity", "") or "Unknown"
            if not event:
                continue
            expires_raw = props.get("expires")
            expires = None
            if expires_raw:
                try:
                    expires = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
                except ValueError:
                    expires = None
            instruction = props.get("instruction") or props.get("description")
            alerts.append(AlertSummary(headline=event, severity=severity, expires=expires, instruction=instruction))
        return alerts
