from __future__ import annotations

import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from pathlib import Path
from string import Template
from typing import Iterable, Mapping, Optional
from config import Configuration
from core.utility import session_id_var, request_id_var
# ----------------------------------------------------------------------------------------------------------------------------

@dataclass
class SmtpConfig:
    host: str
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    use_starttls: bool = True  # STARTTLS (port 587)
    use_ssl: bool = False      # SMTPS (port 465) - set True if you need SSL

def render_feedback_html_template(template_path: str | Path, context: Mapping[str, object]) -> str:
    """
    Load an HTML template from disk and inject values using string.Template.
    All dynamic values are HTML-escaped for safety.
    Missing keys are rendered as empty strings.
    """
    template_text = Path(template_path).read_text(encoding="utf-8")

    rendered_html = template_text \
                            .replace("@session", context.get("session", "-")) \
                            .replace("@requestId", context.get("requestId", "-")) \
                            .replace("@eventType", context.get("eventType", "-")) \
                            .replace("@eventSubType", context.get("eventSubType", "-")) \
                            .replace("@createdOn", context.get("createdOn", "-")) \
                            .replace("@dafileid", context.get("dafileid", "-")) \
                            .replace("@filename", context.get("filename", "-")) \
                            .replace("@feedback", str(context.get("feedback", "-"))) 
    return rendered_html

def render_html_template(template_path: str | Path, context: Mapping[str, object]) -> str:
    """
    Load an HTML template from disk and inject values using string.Template.
    All dynamic values are HTML-escaped for safety.
    Missing keys are rendered as empty strings.
    """
    template_text = Path(template_path).read_text(encoding="utf-8")

    rendered_html = template_text \
                            .replace("@session", context.get("session", "-")) \
                            .replace("@requestId", context.get("requestId", "-")) \
                            .replace("@eventType", context.get("eventType", "-")) \
                            .replace("@eventSubType", context.get("eventSubType", "-")) \
                            .replace("@createdOn", context.get("createdOn", "-")) \
                            .replace("@error_text", str(context.get("error_text", "-"))) 
    return rendered_html

def build_email(
    *,
    sender: str,
    recipients: Iterable[str],
    subject: str,
    html_body: str,
    text_fallback: Optional[str] = None,
    charset: str = "utf-8",
) -> MIMEMultipart:
    """
    Build a MIME multipart email with HTML and optional plain-text fallback.
    """
    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    if text_fallback:
        msg.attach(MIMEText(text_fallback, "plain", _charset=charset))

    msg.attach(MIMEText(html_body, "html", _charset=charset))
    return msg


def send_email(msg: MIMEMultipart, smtp: SmtpConfig) -> None:
    """
    Send a prepared email using the given SMTP configuration.
    """

    if smtp.use_ssl:
        # SMTPS (e.g., port 465)
        with smtplib.SMTP_SSL(smtp.host, smtp.port) as server:
            if smtp.username and smtp.password:
                server.login(smtp.username, smtp.password)
            server.send_message(msg)

    else:
        # STARTTLS (e.g., port 587)
        with smtplib.SMTP(smtp.host, smtp.port) as server:
            server.ehlo()
            if smtp.use_starttls:
                server.starttls()
                server.ehlo()
            if smtp.username and smtp.password:
                server.login(smtp.username, smtp.password)
            server.send_message(msg)


def send_consultiq_notification(
    *,
    smtp: SmtpConfig,
    sender: str,
    recipients: Iterable[str],
    subject: str,
    context: Mapping[str, object],
    template_path: str | Path,
    include_text_fallback: bool = True,
) -> None:
    """
    High-level helper that:
      - renders the HTML template with given context
      - builds a MIME message (with optional text fallback)
      - sends the message via SMTP

    Example context keys expected by the default template:
      session, request_id, event_type, event_sub_type, request_time_utc, error_text
    """
    html_body = render_html_template(template_path, context)

    text_fallback = None
    if include_text_fallback:
        # Create a simple text alternative using the same context (no HTML)
        fields = [
            ("Session", "session"),
            ("Request", "requestId"),
            ("Event Type", "eventType"),
            ("Event Sub Type", "eventSubType"),
            ("Request Time (UTC)", "createdOn"),
            ("Error", "error_text"),
        ]
        # Use str() and default to "" if missing
        lines = ["Consult IQ – Notification", ""]
        for label, key in fields:
            value = context.get(key, "")
            lines.append(f"{label}: {'' if value is None else str(value)}")
        text_fallback = "\n".join(lines)

    msg = build_email(
        sender=sender,
        recipients=list(recipients),
        subject=subject,
        html_body=html_body,
        text_fallback=text_fallback,
    )

    send_email(msg, smtp)

def notify_failures(context, error_heading):
    MODULE_DIR = Path(__file__).resolve().parent
    
    TEMPLATE_PATH = MODULE_DIR.parent / "core" / "templates" / "emailNotificationTemplate.html"
    
    cfg = Configuration()
    cfg.load_active_config()
    if not cfg.NOTIFICATION_SWITCH:
        return

    smtp = SmtpConfig(
        host=cfg.SMTP_HOST,
        port=587,
        username=cfg.SVC_ACCOUNT_EMAIL,
        password=cfg.SVC_ACCOUNT_PASSWORD_EMAIL,
        use_starttls=True,
        use_ssl=False,
    )

    context['session'] = session_id_var.get()
    # context = {
    #     "session": "XYZ",
    #     "request_id": "1234",
    #     "event_type": "IP_COPY_...",
    #     "event_sub_type": "IP_COPY_...",
    #     "request_time_utc": "2025-12-01 00:00:00",
    #     "error_text": "xyz....",
    # }
    
    send_consultiq_notification(
        smtp=smtp,
        sender=cfg.SVC_ACCOUNT_EMAIL,
        recipients=cfg.NOTIFICATION_DL.split(";"),
        subject=f"Consult IQ | {cfg.ENVIRONMENT} | {error_heading}",
        context=context,
        template_path=TEMPLATE_PATH,
        include_text_fallback=True,
    )

def send_consultiq_feedback_notification(
    *,
    smtp: SmtpConfig,
    sender: str,
    recipients: Iterable[str],
    subject: str,
    context: Mapping[str, object],
    template_path: str | Path,
    include_text_fallback: bool = True,
) -> None:
    """
    High-level helper that:
      - renders the HTML template with given context
      - builds a MIME message (with optional text fallback)
      - sends the message via SMTP

    Example context keys expected by the default template:
      session, request_id, event_type, event_sub_type, request_time_utc, error_text
    """
    html_body = render_feedback_html_template(template_path, context)

    text_fallback = None
    if include_text_fallback:
        # Create a simple text alternative using the same context (no HTML)
        fields = [
            ("Session", "session"),
            ("Request", "requestId"),
            ("Event Type", "eventType"),
            ("Event Sub Type", "eventSubType"),
            ("Request Time (UTC)", "createdOn"),
            ("Da FileId", "dafileid"),
            ("File Name", "filename"),
            ("Feedback", "feedback"),
        ]
        # Use str() and default to "" if missing
        lines = ["Consult IQ – Notification", ""]
        for label, key in fields:
            value = context.get(key, "")
            lines.append(f"{label}: {'' if value is None else str(value)}")
        text_fallback = "\n".join(lines)

    msg = build_email(
        sender=sender,
        recipients=list(recipients),
        subject=subject,
        html_body=html_body,
        text_fallback=text_fallback,
    )

    send_email(msg, smtp)


def notify_feedbacks(context):
    MODULE_DIR = Path(__file__).resolve().parent
    
    TEMPLATE_PATH = MODULE_DIR.parent / "core" / "templates" / "FeedbackemailNotificationTemplate.html"
    
    cfg = Configuration()
    cfg.load_active_config()
    if not cfg.NOTIFICATION_SWITCH:
        return

    smtp = SmtpConfig(
        host=cfg.SMTP_HOST,
        port=587,
        username=cfg.SVC_ACCOUNT_EMAIL,
        password=cfg.SVC_ACCOUNT_PASSWORD_EMAIL,
        use_starttls=True,
        use_ssl=False,
    )

    context['session'] = session_id_var.get()
    # context = {
    #     "session": "XYZ",
    #     "request_id": "1234",
    #     "event_type": "IP_COPY_...",
    #     "event_sub_type": "IP_COPY_...",
    #     "request_time_utc": "2025-12-01 00:00:00",
    #     "error_text": "xyz....",
    # }
    
    send_consultiq_feedback_notification(
        smtp=smtp,
        sender=cfg.SVC_ACCOUNT_EMAIL,
        recipients=cfg.NOTIFICATION_DL.split(";"),
        subject=f"Consult IQ GTL Feedback | {cfg.ENVIRONMENT} | {context['filename']}",
        context=context,
        template_path=TEMPLATE_PATH,
        include_text_fallback=True,
    )