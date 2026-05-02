from app import create_app
from datetime import datetime, timezone, timedelta
from api_clients import fetch_bitget_all_transactions
from api_clients import BitgetTransactionProcessor
from models import InvestmentPlatform
from extensions import db

app = create_app()

with app.app_context():
    p = InvestmentPlatform.query.filter_by(id=23).first()
    print(f"Platform: {p.name}")
    
    # Get existing tx IDs
    existing_tx_ids = {tx.exchange_tx_id for tx in p.transactions}
    print(f"Existing transactions: {len(existing_tx_ids)}")
    
    # Fetch data
    start = datetime.now(timezone.utc) - timedelta(days=365)
    end = datetime.now(timezone.utc)
    
    print("Fetching transactions...")
    txs = fetch_bitget_all_transactions(
        p.api_key, p.api_secret, p.passphrase,
        start_time_dt=start, end_time_dt=end
    )
    
    print(f"Futures trades: {len(txs.get('futures_trades', []))}")
    print(f"First trade ID: {txs.get('futures_trades', [{}])[0].get('tradeId') if txs.get('futures_trades') else 'None'}")
    
    # Process
    print("Processing...")
    processor = BitgetTransactionProcessor(p, existing_tx_ids)
    processor.process(txs)
    
    print(f"Added count: {processor.added_count}")
    
    # Commit
    db.session.commit()
    print("Done!")