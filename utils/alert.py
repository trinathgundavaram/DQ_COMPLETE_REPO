"""
utils/alert.py
--------------
Alert dispatcher — Microsoft Teams (Adaptive Cards) and Office365 SMTP.

Fix #7 (v2): All configuration is read lazily inside send_alert() rather
than at module import time.  Credentials are never stored as module-level
strings; they're read from env vars on each call so rotation takes effect
immediately without a process restart.

Fix #8 (v2): Teams payload migrated from deprecated MessageCard format to
Adaptive Cards (application/vnd.microsoft.card.adaptive), which is the
current Microsoft-recommended format for Teams incoming webhooks.
"""

import json
import logging
import os
import smtplib
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

# Severity → Adaptive Card colour token
_LEVEL_COLOUR = {
    "ERROR": "Attention",
    "WARN":  "Warning",
    "INFO":  "Good",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_alert(message: str, level: str = "INFO"):
    """
    Dispatch an alert via Microsoft Teams and/or SMTP email.

    Configuration is read from env vars on every call (lazy) so that
    credentials rotated after process start are picked up automatically.
    Failures in individual channels are logged but never re-raised.

    This is the global/fallback channel — used when no row in
    dq_notification_routes matches. Prefer send_alert_to() for
    audience-routed notifications (see rules_engine/reporting.py).
    """
    cfg = _load_config()
    level = (level or "INFO").upper()
    logger.info("ALERT [%s]: %s", level, message[:200])

    if cfg["teams_url"]:
        try:
            _send_teams(message, level, cfg["teams_url"])
        except Exception as exc:
            logger.error("Teams alert failed: %s", exc)
    else:
        logger.debug("DQ_TEAMS_WEBHOOK_URL not set — skipping Teams alert.")

    if cfg["email_from"] and cfg["email_to"]:
        try:
            _send_email(message, level, cfg)
        except Exception as exc:
            logger.error("Email alert failed: %s", exc)
    else:
        logger.debug("Email credentials/recipients not configured — skipping email.")


def send_alert_to(message: str, level: str, channel_type: str, destination: str):
    """
    Dispatch an alert to an EXPLICIT destination — used by rules_engine/reporting.py
    for audience-routed notifications (dq_notification_routes rows), as
    opposed to send_alert() which always uses the single global env-configured
    channel.

    Parameters
    ----------
    channel_type : "TEAMS" | "EMAIL"
    destination   : webhook URL (TEAMS) or comma-separated email list (EMAIL)
    """
    level = (level or "INFO").upper()
    channel_type = (channel_type or "").upper()
    cfg = _load_config()

    try:
        if channel_type == "TEAMS":
            _send_teams(message, level, destination)
        elif channel_type == "EMAIL":
            to_list = [e.strip() for e in destination.split(",") if e.strip()]
            routed_cfg = dict(cfg)
            routed_cfg["email_to"] = to_list
            _send_email(message, level, routed_cfg)
        else:
            logger.error("Unknown channel_type '%s' in notification route — message dropped.",
                         channel_type)
    except Exception as exc:
        logger.error("Routed alert to %s (%s) failed: %s", destination, channel_type, exc)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    """Read all alert configuration from env vars at call time.

    DQ_EMAIL_SSL_MODE controls how the SMTP connection is secured:
        starttls  — STARTTLS upgrade (default; port 587; Office 365 / Gmail)
        ssl       — Direct SSL/TLS connection (port 465)
        none      — Plain SMTP, no encryption (corporate relay with IP allowlist)
    """
    raw_to   = os.getenv("DQ_EMAIL_TO", "")
    ssl_mode = os.getenv("DQ_EMAIL_SSL_MODE", "starttls").lower()
    default_port = {"ssl": 465, "none": 25}.get(ssl_mode, 587)
    return {
        "teams_url":      os.getenv("DQ_TEAMS_WEBHOOK_URL", ""),
        "smtp_server":    os.getenv("DQ_SMTP_SERVER", "smtp.office365.com"),
        "smtp_port":      int(os.getenv("DQ_SMTP_PORT", str(default_port))),
        "email_from":     os.getenv("DQ_EMAIL_FROM", ""),
        "email_password": os.getenv("DQ_EMAIL_PASSWORD", ""),
        "email_to":       [e.strip() for e in raw_to.split(",") if e.strip()],
        "ssl_mode":       ssl_mode,
    }


def _send_teams(message: str, level: str, webhook_url: str):
    """
    Send an Adaptive Card to a Teams channel via incoming webhook.

    Adaptive Cards replaced the deprecated MessageCard format (2022).
    Webhook URL: Teams channel → Connectors → Incoming Webhook.
    """
    colour = _LEVEL_COLOUR.get(level, "Accent")
    title  = f"Data Quality Alert — {level}"

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Medium",
                            "weight": "Bolder",
                            "color": colour,
                            "text": title,
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": message,
                            "wrap": True,
                            "fontType": "Monospace",
                        },
                    ],
                },
            }
        ],
    }

    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)
    logger.info("Teams Adaptive Card alert sent [%s].", level)


def _send_email(message: str, level: str, cfg: dict):
    """Send a plain-text email alert.

    Supports three SSL modes via DQ_EMAIL_SSL_MODE:
        starttls  — STARTTLS upgrade after connect (default)
        ssl       — Direct SSL/TLS connection (smtplib.SMTP_SSL)
        none      — Plain SMTP, no encryption; no login if password is absent
    """
    subject = f"[DQ ALERT] [{level}] Data Quality Framework Notification"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["email_from"]
    msg["To"]      = ", ".join(cfg["email_to"])
    msg.attach(MIMEText(message, "plain"))

    ssl_mode = cfg.get("ssl_mode", "starttls")

    if ssl_mode == "ssl":
        # Direct SSL connection — no STARTTLS upgrade needed
        with smtplib.SMTP_SSL(cfg["smtp_server"], cfg["smtp_port"]) as server:
            server.ehlo()
            if cfg["email_password"]:
                server.login(cfg["email_from"], cfg["email_password"])
            server.sendmail(cfg["email_from"], cfg["email_to"], msg.as_string())

    elif ssl_mode == "none":
        # Plain SMTP — corporate relay that allows by IP, no auth
        with smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"]) as server:
            server.ehlo()
            if cfg["email_password"]:
                server.login(cfg["email_from"], cfg["email_password"])
            server.sendmail(cfg["email_from"], cfg["email_to"], msg.as_string())

    else:
        # Default: STARTTLS
        with smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"]) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            if cfg["email_password"]:
                server.login(cfg["email_from"], cfg["email_password"])
            server.sendmail(cfg["email_from"], cfg["email_to"], msg.as_string())

    logger.info("Email alert sent to %s [%s] (ssl_mode=%s).",
                cfg["email_to"], level, ssl_mode)
