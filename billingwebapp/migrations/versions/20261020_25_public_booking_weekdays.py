"""Configure the weekdays accepting public doctor appointments.

Revision ID: 20261020_25
Revises: 20261010_24
"""
from alembic import op
import sqlalchemy as sa

revision = "20261020_25"
down_revision = "20261010_24"
branch_labels = None
depends_on = None


def upgrade():
    # The additive runtime repair may already have created this column.
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("appointment_booking_settings")}
    if "booking_weekdays" not in columns:
        op.add_column("appointment_booking_settings", sa.Column(
            "booking_weekdays", sa.String(13), nullable=False,
            server_default="0,1,2,3,4,5",
        ))


def downgrade():
    with op.batch_alter_table("appointment_booking_settings") as batch_op:
        batch_op.drop_column("booking_weekdays")
