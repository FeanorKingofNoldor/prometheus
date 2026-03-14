"""
Prometheus v2: Alembic Environment Configuration

This module configures Alembic for running database migrations against
either the historical or runtime databases, depending on the ALEMBIC_DB
environment variable.

Key responsibilities:
- Build SQLAlchemy engine from environment variables
- Configure Alembic context for offline and online modes

Author: Prometheus Team
Created: 2025-11-24
Last Modified: 2025-11-24
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine, pool
from dotenv import load_dotenv

# from prometheus.models import Base  # TODO: attach metadata when models exist

config = context.config

# Load environment variables from project .env so that Alembic uses the
# same database credentials as the application code.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# In Iteration 1 we do not use SQLAlchemy models; metadata will be
# attached in a later iteration when declarative models are introduced.
target_metadata = None


def _get_database_url() -> str:
    """Construct database URL based on ALEMBIC_DB environment variable.

    If ``ALEMBIC_DB=historical`` is set, migrations will run against the
    historical database. Otherwise, the runtime database is used.
    """

    db_selector = os.environ.get("ALEMBIC_DB", "runtime").lower()

    if db_selector == "historical":
        host = os.environ.get("HISTORICAL_DB_HOST", "localhost")
        port = os.environ.get("HISTORICAL_DB_PORT", "5432")
        name = os.environ.get("HISTORICAL_DB_NAME", "prometheus_historical")
        user = os.environ.get("HISTORICAL_DB_USER", "prometheus")
        password = os.environ.get("HISTORICAL_DB_PASSWORD", "")
    else:
        host = os.environ.get("RUNTIME_DB_HOST", "localhost")
        port = os.environ.get("RUNTIME_DB_PORT", "5432")
        name = os.environ.get("RUNTIME_DB_NAME", "prometheus_runtime")
        user = os.environ.get("RUNTIME_DB_USER", "prometheus")
        password = os.environ.get("RUNTIME_DB_PASSWORD", "")

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    In this mode we configure the context only with a URL and do not
    create an Engine. Calls to ``context.execute()`` emit the given
    string to the script output.
    """

    url = _get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we create an Engine and associate a connection with
    the context.
    """

    connectable = create_engine(_get_database_url(), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
