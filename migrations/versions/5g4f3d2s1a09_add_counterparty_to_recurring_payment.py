"""add counterparty to recurring payment

Revision ID: 5g4f3d2s1a09
Revises: f7g6h5i4j3k2
Create Date: 2025-11-20 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5g4f3d2s1a09'
down_revision = 'f7g6h5i4j3k2'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('recurring_payment', schema=None) as batch_op:
        batch_op.add_column(sa.Column('counterparty', sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table('recurring_payment', schema=None) as batch_op:
        batch_op.drop_column('counterparty')
