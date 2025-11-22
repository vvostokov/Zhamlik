from app import create_app
from extensions import db

app = create_app()
with app.app_context():
    with db.engine.connect() as conn:
        # Add category_id to recurring_payment
        conn.execute(db.text("ALTER TABLE recurring_payment ADD COLUMN category_id INTEGER"))
        # Add recurring_payment_id to debt
        conn.execute(db.text("ALTER TABLE debt ADD COLUMN recurring_payment_id INTEGER"))
        conn.commit()
    print("Columns added")