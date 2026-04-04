"""Initial schema — businesses + message_logs.

Revision ID: 001
Revises: None
Create Date: 2025-01-01 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "businesses",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("phone", sa.String, nullable=True),
        sa.Column("address", sa.String, nullable=True),
        sa.Column("website", sa.String, nullable=True),
        sa.Column("email", sa.String, nullable=True),
        sa.Column("status", sa.String, nullable=False, server_default="PENDING"),
        sa.Column("search_query", sa.String, nullable=True),
        sa.Column("rating", sa.String, nullable=True),
        sa.Column("reviews_count", sa.String, nullable=True),
        sa.Column("category", sa.String, nullable=True),
        sa.Column("filter_reason", sa.String, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_businesses_status", "businesses", ["status"])
    op.create_index("idx_businesses_query", "businesses", ["search_query"])

    op.create_table(
        "message_logs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "business_id",
            sa.Integer,
            sa.ForeignKey("businesses.id"),
            nullable=False,
        ),
        sa.Column("status", sa.String, nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("message_template", sa.String, nullable=True),
    )
    op.create_index("idx_message_logs_biz", "message_logs", ["business_id"])


def downgrade() -> None:
    op.drop_table("message_logs")
    op.drop_table("businesses")
