from app import create_app
from models import Transaction
from extensions import db

app = create_app()
with app.app_context():
    types = db.session.query(Transaction.type).distinct().all()
    print('All transaction types:')
    for t in types:
        print(f'  {t[0]}')