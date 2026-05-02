from app import create_app
from api_clients import _bitget_api_get
from models import InvestmentPlatform

app = create_app()
with app.app_context():
    p = InvestmentPlatform.query.filter_by(id=23).first()
    result = _bitget_api_get(p.api_key, p.api_secret, p.passphrase, '/api/v2/mix/order/fills', {'limit': 3, 'productType': 'usdt-futures'})
    fills = result.get('data', {}).get('fillList', [])
    for f in fills:
        print('TradeId:', f.get('tradeId'))
        print('  symbol:', f.get('symbol'))
        print('  side:', f.get('side'))
        print('  tradeSide:', f.get('tradeSide'))
        print('  profit:', f.get('profit'))
        print('  feeDetail:', f.get('feeDetail'))
        print()