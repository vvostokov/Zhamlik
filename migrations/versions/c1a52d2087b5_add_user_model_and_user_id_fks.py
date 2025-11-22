"""add user model and user_id fks

Revision ID: c1a52d2087b5
Revises: 5g4f3d2s1a09
Create Date: 2025-11-20 02:46:02.528852

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c1a52d2087b5'
down_revision = '5g4f3d2s1a09'
branch_labels = None
depends_on = None


def upgrade():
    # 1. Create User Table
    op.create_table('user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('username', sa.String(length=64), nullable=False),
        sa.Column('email', sa.String(length=120), nullable=True),
        sa.Column('password_hash', sa.String(length=256), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email'),
        sa.UniqueConstraint('username')
    )

    # 2. Add user_id column and FK to existing tables
    tables = [
        'investment_platform', 
        'transaction', 
        'account', 
        'category', 
        'debt', 
        'banking_transaction', 
        'recurring_payment'
    ]
    
    for table in tables:
        with op.batch_alter_table(table, schema=None) as batch_op:
            # Add column as nullable first
            batch_op.add_column(sa.Column('user_id', sa.Integer(), nullable=True))
            batch_op.create_foreign_key(f'fk_{table}_user_id', 'user', ['user_id'], ['id'])


def downgrade():
    tables = [
        'investment_platform', 
        'transaction', 
        'account', 
        'category', 
        'debt', 
        'banking_transaction', 
        'recurring_payment'
    ]
    
    for table in tables:
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.drop_constraint(f'fk_{table}_user_id', type_='foreignkey')
            batch_op.drop_column('user_id')

    op.drop_table('user')
