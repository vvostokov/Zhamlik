import os
os.environ['FLASK_ENV'] = 'production'
from app import create_app
from datetime import datetime, timedelta, timezone
from api_clients import fetch_bitget_all_transactions
from api_clients import BitgetTransactionProcessor
from models import InvestmentPlatform

app = create_app()
with app.app_context():
    p = InvestmentPlatform.query.filter_by(id=17).first()
    print(f"Syncing {p.name}...")
    
    start_time_dt = datetime.now(timezone.utc) - timedelta(days=365)
    end_time_dt = datetime.now(timezone.utc)
    
    txs = fetch_bitget_all_transactions(
        p.api_key, p.api_secret, p.passphrase,
        start_time_dt=start_time_dt, end_time_dt=end_time_dt
    )
    
    print(f"Got {len(txs.get('futures_trades', []))} futures trades")
    
    # Get existing tx IDs
    from models import Transaction
    existing_tx_ids = {tx.exchange_tx_id for tx in p.transactions}
    print(f"Existing: {len(existing_tx_ids)}")
    
    # Process
    processor = BitgetTransactionProcessor(p, existing_tx_ids)
    processor.process(txs)
    
    print(f"Added: {processor.added_count}")
    
    from extensions import db
    db.session.commit()
    print("Done!")