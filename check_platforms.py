from app import create_app
from models import InvestmentPlatform
app = create_app()
with app.app_context():
    for p in InvestmentPlatform.query.filter(InvestmentPlatform.id.in_([17, 23])).all():
        print(f"ID {p.id}: name='{p.name}' lower='{p.name.lower()}'")