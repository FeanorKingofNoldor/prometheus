"""embedding tables

Revision ID: 0005
Revises: 0004
Create Date: 2025-11-24

This migration creates embedding tables used by the encoder layer:

- text_embeddings
- numeric_window_embeddings
- joint_embeddings
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create embedding tables for text, numeric, and joint embeddings."""

    # text_embeddings
    op.create_table(
        "text_embeddings",
        sa.Column("embedding_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("source_type", sa.String(length=32), nullable=False),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("vector", postgresql.BYTEA, nullable=True),
        sa.Column("vector_ref", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_text_embeddings_source_model",
        "text_embeddings",
        ["source_type", "source_id", "model_id"],
        unique=True,
    )

    # numeric_window_embeddings
    op.create_table(
        "numeric_window_embeddings",
        sa.Column("embedding_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("window_spec", postgresql.JSONB, nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("vector", postgresql.BYTEA, nullable=True),
        sa.Column("vector_ref", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_numeric_embeddings_entity_date_model",
        "numeric_window_embeddings",
        ["entity_type", "entity_id", "as_of_date", "model_id"],
    )

    # joint_embeddings
    op.create_table(
        "joint_embeddings",
        sa.Column("joint_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("joint_type", sa.String(length=32), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("entity_scope", postgresql.JSONB, nullable=False),
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("vector", postgresql.BYTEA, nullable=True),
        sa.Column("vector_ref", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_joint_embeddings_type_date_model",
        "joint_embeddings",
        ["joint_type", "as_of_date", "model_id"],
    )


def downgrade() -> None:
    """Drop embedding tables and indexes."""

    op.drop_index("idx_joint_embeddings_type_date_model", table_name="joint_embeddings")
    op.drop_table("joint_embeddings")

    op.drop_index("idx_numeric_embeddings_entity_date_model", table_name="numeric_window_embeddings")
    op.drop_table("numeric_window_embeddings")

    op.drop_index("idx_text_embeddings_source_model", table_name="text_embeddings")
    op.drop_table("text_embeddings")
