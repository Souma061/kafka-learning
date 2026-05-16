import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/kafka_learning",
)

# Resend email config
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "onboarding@resend.dev")

# Max emails per second the email-service consumer will process.
# Lowering this makes Kafka's buffering advantage obvious under load.
EMAIL_RATE_LIMIT_PER_SECOND = float(os.getenv("EMAIL_RATE_LIMIT_PER_SECOND", "2"))

# Outbox relay poll interval in seconds
OUTBOX_POLL_INTERVAL = float(os.getenv("OUTBOX_POLL_INTERVAL", "0.1"))
OUTBOX_REDIS_MAX_EVENTS = int(os.getenv("OUTBOX_REDIS_MAX_EVENTS", "1000"))
OUTBOX_POSTGRES_BATCH_SIZE = int(os.getenv("OUTBOX_POSTGRES_BATCH_SIZE", "100"))
