from app import create_app
from extensions import db

app = create_app()
with app.app_context():
    with db.engine.connect() as conn:
        result = conn.execute(db.text('SELECT version_num FROM alembic_version')).fetchone()
        print(result[0] if result else 'No version')