import os
import json
import smtplib
import logging
import urllib.request
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

# --- Configuration (set via environment variables) ---
TEAMS_WEBHOOK_URL = os.getenv("DQ_TEAMS_WEBHOOK_URL", "")
SMTP_SERVER       = os.getenv("DQ_SMTP_SERVER", "smtp.office365.com")
SMTP_PORT         = int(os.getenv("DQ_SMTP_PORT", "587"))
EMAIL_FROM        = os.getenv("DQ_EMAIL_FROM", "")
EMAIL_PASSWORD    = os.getenv("DQ_EMAIL_PASSWORD", "")
EMAIL_TO_RAW      = os.getenv("DQ_EMAIL_TO", "")
EMAIL_TO          = [e.strip() for e in EMAIL_TO_RAW.split(",") if e.strip()]


def send_alert(message: str, level: str = "INFO"):
    """
    Dispatch an alert via Microsoft Teams webhook and/or email.
    Failures in individual channels are logged but do not raise.
    """
    subject = f"[DQ ALERT] [{level}] Data Quality Framework Notification"
    logger.info("ALERT [%s]: %s", level, message)

    if TEAMS_WEBHOOK_URL:
        try:
            _send_teams_alert(message, level)
        except Exception as exc:
            logger.error("Teams alert failed: %s", exc)
    else:
        logger.warning("DQ_TEAMS_WEBHOOK_URL not set — skipping Teams alert.")

    if EMAIL_FROM and EMAIL_TO:
        try:
            _send_email_alert(message, subject)
        except Exception as exc:
            logger.error("Email alert failed: %s", exc)
    else:
        logger.warning("Email credentials/recipients not set — skipping email alert.")


def _send_teams_alert(message: str, level: str):
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "DQ Framework Alert",
        "themeColor": "FF0000" if level == "ERROR" else "FFA500",
        "title": f"Data Quality Alert - {level}",
        "text": message,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        TEAMS_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)
    logger.info("Teams alert sent.")


def _send_email_alert(message: str, subject: str):
    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = ", ".join(EMAIL_TO)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    logger.info("Email alert sent to %s.", EMAIL_TO)
