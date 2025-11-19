"""add counterparty to banking transaction

Revision ID: f7g6h5i4j3k2
Revises: e89840caea24
Create Date: 2025-11-20 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f7g6h5i4j3k2'
down_revision = 'e89840caea24'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('banking_transaction', schema=None) as batch_op:
        batch_op.add_column(sa.Column('counterparty', sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table('banking_transaction', schema=None) as batch_op:
        batch_op.drop_column('counterparty')