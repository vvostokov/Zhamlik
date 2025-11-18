"""encrypt_existing_api_secrets_and_passphrases

Revision ID: e2b382f21a07
Revises: 695a643d9d8e
Create Date: 2025-11-15 23:36:34.033270

"""
from alembic import op
import sqlalchemy as sa
from cryptography.fernet import Fernet
import os


# revision identifiers, used by Alembic.
revision = 'e2b382f21a07'
down_revision = '695a643d9d8e'
branch_labels = None
depends_on = None


def upgrade():
    # Шифруем существующие api_secret и passphrase
    connection = op.get_bind()
    
    # Получаем FERNET_KEY из переменных окружения или генерируем временный
    fernet_key = os.environ.get('FERNET_KEY')
    if not fernet_key:
        raise Exception("FERNET_KEY environment variable is required for this migration")
    
    f = Fernet(fernet_key.encode())
    
    # Шифруем api_secret
    result = connection.execute(sa.text("SELECT id, api_secret FROM investment_platform WHERE api_secret IS NOT NULL AND api_secret != ''"))
    for row in result:
        try:
            # Проверяем, уже ли зашифрованы данные
            encrypted_secret = f.encrypt(row.api_secret.encode()).decode()
            connection.execute(
                sa.text("UPDATE investment_platform SET api_secret = :secret WHERE id = :id"),
                {"secret": encrypted_secret, "id": row.id}
            )
        except Exception as e:
            print(f"Failed to encrypt api_secret for platform {row.id}: {e}")
    
    # Шифруем passphrase
    result = connection.execute(sa.text("SELECT id, passphrase FROM investment_platform WHERE passphrase IS NOT NULL AND passphrase != ''"))
    for row in result:
        try:
            # Проверяем, уже ли зашифрованы данные
            encrypted_passphrase = f.encrypt(row.passphrase.encode()).decode()
            connection.execute(
                sa.text("UPDATE investment_platform SET passphrase = :passphrase WHERE id = :id"),
                {"passphrase": encrypted_passphrase, "id": row.id}
            )
        except Exception as e:
            print(f"Failed to encrypt passphrase for platform {row.id}: {e}")


def downgrade():
    # Дешифруем api_secret и passphrase (откат миграции)
    connection = op.get_bind()
    
    fernet_key = os.environ.get('FERNET_KEY')
    if not fernet_key:
        raise Exception("FERNET_KEY environment variable is required for this migration")
    
    f = Fernet(fernet_key.encode())
    
    # Дешифруем api_secret
    result = connection.execute(sa.text("SELECT id, api_secret FROM investment_platform WHERE api_secret IS NOT NULL AND api_secret != ''"))
    for row in result:
        try:
            decrypted_secret = f.decrypt(row.api_secret.encode()).decode()
            connection.execute(
                sa.text("UPDATE investment_platform SET api_secret = :secret WHERE id = :id"),
                {"secret": decrypted_secret, "id": row.id}
            )
        except Exception as e:
            print(f"Failed to decrypt api_secret for platform {row.id}: {e}")
    
    # Дешифруем passphrase
    result = connection.execute(sa.text("SELECT id, passphrase FROM investment_platform WHERE passphrase IS NOT NULL AND passphrase != ''"))
    for row in result:
        try:
            decrypted_passphrase = f.decrypt(row.passphrase.encode()).decode()
            connection.execute(
                sa.text("UPDATE investment_platform SET passphrase = :passphrase WHERE id = :id"),
                {"passphrase": decrypted_passphrase, "id": row.id}
            )
        except Exception as e:
            print(f"Failed to decrypt passphrase for platform {row.id}: {e}")
