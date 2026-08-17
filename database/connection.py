"""Minimal, reusable PostgreSQL connection foundation.

Scope (Step 3.3.2): connection plumbing only.
- Environment-based configuration (POSTGRES_HOST / POSTGRES_PORT /
  POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD, per the approved Step
  3.2.4 environment variables).
- PostgreSQL connection creation.
- A safe transaction context manager (commit on success, rollback on
  exception).
- Clean connection closing.

This module deliberately does NOT implement CSV loading, checksum
processing, truncate/reload, or any ingestion orchestration — that is
Step 3.3.3+.

psycopg2 is imported lazily inside the functions that need it so that this
module can still be imported (e.g. for type-checking or by tests that only
exercise the configuration piece) in environments where the driver isn't
installed.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator


class DatabaseConfigError(RuntimeError):
    """Raised when required database configuration is missing/invalid."""


class DatabaseConnectionError(RuntimeError):
    """Raised when a PostgreSQL connection cannot be established."""


@dataclass(frozen=True)
class DBConfig:
    """Database connection configuration, sourced from environment variables."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DBConfig":
        """Build a DBConfig from the approved environment variables.

        Required: POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
        Optional: POSTGRES_PORT (defaults to 5432).
        """
        missing = [
            name
            for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER",
                         "POSTGRES_PASSWORD")
            if not os.environ.get(name)
        ]
        if missing:
            raise DatabaseConfigError(
                "Missing required environment variable(s): "
                f"{', '.join(missing)}. See .env.example."
            )

        raw_port = os.environ.get("POSTGRES_PORT", "5432")
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise DatabaseConfigError(
                f"POSTGRES_PORT must be an integer, got: {raw_port!r}"
            ) from exc

        return cls(
            host=os.environ["POSTGRES_HOST"],
            port=port,
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

    def as_dsn_kwargs(self) -> dict[str, Any]:
        """Connection kwargs suitable for psycopg2.connect(**kwargs).

        Note: intentionally excludes the password from any __repr__/logging
        path — callers should not print this config.
        """
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }

    def __repr__(self) -> str:  # pragma: no cover - trivial
        # Never expose the password, even accidentally, in logs/tracebacks.
        return (
            f"DBConfig(host={self.host!r}, port={self.port!r}, "
            f"dbname={self.dbname!r}, user={self.user!r}, password='***')"
        )


def get_connection(config: DBConfig | None = None):
    """Create and return a new psycopg2 connection.

    Autocommit is left OFF (the default) so callers control transaction
    boundaries explicitly, typically via the `transaction()` context manager
    below.
    """
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise DatabaseConnectionError(
            "psycopg2 is not installed. Add psycopg2-binary to the "
            "environment (see requirements.txt) before connecting to "
            "PostgreSQL."
        ) from exc

    cfg = config or DBConfig.from_env()
    try:
        return psycopg2.connect(**cfg.as_dsn_kwargs())
    except psycopg2.OperationalError as exc:
        raise DatabaseConnectionError(
            f"Could not connect to PostgreSQL at {cfg.host}:{cfg.port}/"
            f"{cfg.dbname}: {exc}"
        ) from exc


def close_connection(conn) -> None:
    """Close a connection, swallowing errors from an already-closed connection."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception:  # pragma: no cover - defensive cleanup only
        pass


@contextmanager
def transaction(config: DBConfig | None = None) -> Iterator[Any]:
    """Context manager yielding a connection wrapped in a single transaction.

    Commits on clean exit, rolls back and re-raises on any exception, and
    always closes the connection afterwards.

    Usage:
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    conn = get_connection(config)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        close_connection(conn)
