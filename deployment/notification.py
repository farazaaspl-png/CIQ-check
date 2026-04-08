from core.emailNotification import SmtpConfig, send_consultiq_notification
from pathlib import Path
from config import Config as cfg

def notify():

    MODULE_DIR = Path(__file__).resolve().parent
    TEMPLATE_PATH = MODULE_DIR.parent / "core" / "templates" / "emailNotificationTemplate.html"

    smtp = SmtpConfig(
        host=cfg.SMTP_HOST,
        port=587,
        username=cfg.SVC_ACCOUNT_EMAIL,
        password=cfg.SVC_ACCOUNT_PASSWORD_EMAIL,
        use_starttls=True,
        use_ssl=False,
    )

    context = {
        "session": "XYZ",
        "request_id": "1234",
        "event_type": "IP_COPY_...",
        "event_sub_type": "IP_COPY_...",
        "request_time_utc": "2025-12-01 00:00:00",
        "error_text": "xyz....",
    }

    send_consultiq_notification(
        smtp=smtp,
        sender=cfg.SVC_ACCOUNT_EMAIL,
        recipients=["Punit.Gour@test.com"],
        subject="Consult IQ | ${cfg.ENVIRONMENT} | <Error heading>",
        context=context,
        template_path=TEMPLATE_PATH,
        include_text_fallback=True,
    )

if __name__ == "__main__":
    notify()