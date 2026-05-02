import os
os.environ['FLASK_ENV'] = 'production'
from app import create_app

app = create_app()
with app.app_context():
    from api_clients import _bitget_api_get
    from models import InvestmentPlatform
    
    p = InvestmentPlatform.query.filter_by(id=17).first()
    print(f"Testing {p.name}...")
    
    result = _bitget_api_get(p.api_key, p.api_secret, p.passphrase, '/api/v2/mix/order/fills', {'limit': 5, 'productType': 'usdt-futures'})
    if result:
        fills = result.get('data', {}).get('fillList', [])
        print(f"Futures fills: {len(fills)}")
        for f in fills[:2]:
            print(f"  {f.get('symbol')}: {f.get('side')} {f.get('baseVolume')} @ {f.get('price')}")