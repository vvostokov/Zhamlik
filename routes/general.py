from datetime import date, timedelta
from decimal import Decimal
from collections import defaultdict
from flask import render_template
from sqlalchemy.orm import joinedload

from routes import main_bp
from extensions import db
from models import InvestmentAsset, InvestmentPlatform, SecuritiesPortfolioHistory, CryptoPortfolioHistory, Account, Debt, Transaction, BankingTransaction, HistoricalPriceCache
from flask_login import login_required, current_user
from services.common import _get_currency_rates

def _calculate_portfolio_changes(history_records: list) -> dict:
    """Рассчитывает процентные изменения портфеля для разных периодов."""
    changes = {'1d': None, '7d': None, '30d': None, '180d': None, '365d': None}
    if not history_records:
        return changes

    history_by_date = {record.date: record.total_value_rub for record in history_records}
    
    # Находим самую последнюю доступную дату в истории как "текущую"
    latest_date = max(history_by_date.keys())
    latest_val = history_by_date[latest_date]

    periods = {'1d': 1, '7d': 7, '30d': 30, '180d': 180, '365d': 365}
    for period_name, days_ago in periods.items():
        past_date = latest_date - timedelta(days=days_ago)
        past_val = history_by_date.get(past_date)
        
        if past_val is not None and past_val > 0:
            change_pct = ((latest_val - past_val) / past_val) * 100
            changes[period_name] = change_pct
            
    return changes

@main_bp.route('/')
@login_required
def index():
    # --- Константы и курсы ---
    currency_rates_to_rub = _get_currency_rates()

    # --- 1. Сводка по портфелю ценных бумаг ---
    securities_assets = InvestmentAsset.query.join(InvestmentPlatform).filter(InvestmentPlatform.platform_type == 'stock_broker', InvestmentPlatform.user_id == current_user.id).all()
    securities_total_rub = sum(
        (asset.quantity or 0) * (asset.current_price or 0) * currency_rates_to_rub.get(asset.currency_of_price, Decimal(1.0))
        for asset in securities_assets
    )
    # Расчет изменений для портфеля ЦБ
    # Note: SecuritiesPortfolioHistory is currently global/not linked to user_id directly in model, 
    # but usually history is aggregate. If we want per user history, we need to add user_id to History models too.
    # For now, assuming single user or global history is okay, OR we need to filter history if possible.
    # However, History tables don't have user_id yet. I will skip filtering history tables for now 
    # as adding user_id to them requires more complex migration for existing data.
    # BUT, the prompt said "application is essentially single user". So maybe it's fine for history to remain global for now
    # or assume it belongs to the logged in user if we are migrating to multi-user.
    # Given the scope, I will filter what has user_id.
    
    securities_history_start_date = date.today() - timedelta(days=366)
    securities_history = SecuritiesPortfolioHistory.query.filter(SecuritiesPortfolioHistory.date >= securities_history_start_date).order_by(SecuritiesPortfolioHistory.date.asc()).all()
    securities_changes = _calculate_portfolio_changes(securities_history)

    # --- 2. Сводка по крипто-портфелю ---
    crypto_assets = InvestmentAsset.query.join(InvestmentPlatform).filter(InvestmentPlatform.platform_type == 'crypto_exchange', InvestmentPlatform.user_id == current_user.id).all()
    crypto_total_usdt = sum((asset.quantity or 0) * (asset.current_price or 0) for asset in crypto_assets)
    crypto_total_rub = crypto_total_usdt * currency_rates_to_rub['USDT']

    # Расчет изменений для крипто-портфеля за разные периоды
    start_date_query = date.today() - timedelta(days=366)
    crypto_history = CryptoPortfolioHistory.query.filter(CryptoPortfolioHistory.date >= start_date_query).order_by(CryptoPortfolioHistory.date.asc()).all()
    crypto_changes = _calculate_portfolio_changes(crypto_history)

    # --- 3. Сводка по банковским счетам (включая кредитные карты) ---
    bank_accounts = Account.query.filter(Account.account_type.in_(['bank_account', 'deposit', 'bank_card', 'credit']), Account.user_id == current_user.id).all()
    banking_total_rub = Decimal(0)
    for acc in bank_accounts:
        value_in_rub = acc.balance * currency_rates_to_rub.get(acc.currency, Decimal(1.0))
        if acc.account_type == 'credit':
            banking_total_rub -= value_in_rub # Вычитаем долг по кредитке
        else:
            banking_total_rub += value_in_rub # Прибавляем активы
    
    # Список вкладов и накопительных счетов для отображения
    deposits_and_savings = Account.query.filter(
        Account.account_type.in_(['deposit', 'bank_account']),
        Account.is_active == True,
        Account.user_id == current_user.id
    ).order_by(Account.balance.desc()).all()

    # --- 4. Сводка по долгам ---
    i_owe_list = Debt.query.filter_by(debt_type='i_owe', status='active', user_id=current_user.id).all()
    owed_to_me_list = Debt.query.filter_by(debt_type='owed_to_me', status='active', user_id=current_user.id).all()
    i_owe_total_rub = sum(d.initial_amount - d.repaid_amount for d in i_owe_list)
    owed_to_me_total_rub = sum(d.initial_amount - d.repaid_amount for d in owed_to_me_list)

    # --- 5. Последние операции ---
    last_investment_txs = Transaction.query.join(InvestmentPlatform).filter(InvestmentPlatform.user_id == current_user.id).options(joinedload(Transaction.platform)).order_by(Transaction.timestamp.desc()).limit(7).all()
    last_banking_txs = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(Account.user_id == current_user.id).options(
        joinedload(BankingTransaction.account_ref), 
        joinedload(BankingTransaction.to_account_ref)
    ).order_by(BankingTransaction.date.desc()).limit(7).all()

    combined_txs = []
    for tx in last_investment_txs:
        desc = tx.raw_type or tx.type.capitalize()
        value_str = ""
        if tx.type == 'buy':
            desc = f"Покупка {tx.asset1_ticker}"
            value_str = f"-{tx.asset2_amount:,.2f}".replace(',', ' ') + f" {tx.asset2_ticker}"
        elif tx.type == 'sell':
            desc = f"Продажа {tx.asset1_ticker}"
            value_str = f"+{tx.asset2_amount:,.2f}".replace(',', ' ') + f" {tx.asset2_ticker}"
        elif tx.type == 'deposit':
            desc = f"Депозит {tx.asset1_ticker}"
            value_str = f"+{tx.asset1_amount:,.4f}".replace(',', ' ').rstrip('0').rstrip('.') + f" {tx.asset1_ticker}"
        elif tx.type == 'withdrawal':
            desc = f"Вывод {tx.asset1_ticker}"
            value_str = f"-{tx.asset1_amount:,.4f}".replace(',', ' ').rstrip('0').rstrip('.') + f" {tx.asset1_ticker}"
        elif tx.type == 'transfer':
            desc = f"Перевод {tx.asset1_ticker}"
            value_str = f"{tx.asset1_amount:,.4f}".replace(',', ' ').rstrip('0').rstrip('.') + f" {tx.asset1_ticker}"
        
        combined_txs.append({
            'timestamp': tx.timestamp,
            'description': desc,
            'value': value_str,
            'source': tx.platform.name,
            'is_investment': True,
            'is_positive': None
        })

    for tx in last_banking_txs:
        desc = tx.description or tx.transaction_type.capitalize()
        value_str = ""
        is_positive = None
        if tx.transaction_type == 'expense':
            value_str = f"-{tx.amount:,.2f}".replace(',', ' ') + f" {tx.account_ref.currency}"
            is_positive = False
        elif tx.transaction_type == 'income':
            value_str = f"+{tx.amount:,.2f}".replace(',', ' ') + f" {tx.account_ref.currency}"
            is_positive = True
        elif tx.transaction_type == 'transfer':
            desc = f"Перевод на {tx.to_account_ref.name}"
            value_str = f"-{tx.amount:,.2f}".replace(',', ' ') + f" {tx.account_ref.currency}"
            is_positive = False
        elif tx.transaction_type == 'exchange':
            desc = f"Обмен {tx.account_ref.currency} -> {tx.to_account_ref.currency}"
            value_str = f"+{tx.to_amount:,.2f}".replace(',', ' ') + f" {tx.to_account_ref.currency}"
            is_positive = True
        
        combined_txs.append({
            'timestamp': tx.date,
            'description': desc,
            'value': value_str,
            'source': tx.account_ref.name,
            'is_investment': False,
            'is_positive': is_positive
        })

    combined_txs.sort(key=lambda x: x['timestamp'], reverse=True)
    last_10_transactions = combined_txs[:10]

    # --- 5.1 Последние операции по ЦБ ---
    last_securities_txs_raw = Transaction.query.join(InvestmentPlatform).filter(
        InvestmentPlatform.platform_type == 'stock_broker',
        InvestmentPlatform.user_id == current_user.id
    ).options(joinedload(Transaction.platform)).order_by(Transaction.timestamp.desc()).limit(10).all()

    last_securities_txs = []
    for tx in last_securities_txs_raw:
        desc = tx.raw_type or tx.type.capitalize()
        value_str = ""
        is_positive = None
        if tx.type == 'buy':
            desc = f"Покупка {tx.asset1_ticker}"
            value_str = f"-{tx.asset2_amount:,.2f}".replace(',', ' ') + f" {tx.asset2_ticker}"
            is_positive = False
        elif tx.type == 'sell':
            desc = f"Продажа {tx.asset1_ticker}"
            value_str = f"+{tx.asset2_amount:,.2f}".replace(',', ' ') + f" {tx.asset2_ticker}"
            is_positive = True
        
        last_securities_txs.append({
            'timestamp': tx.timestamp, 'description': desc, 'value': value_str,
            'source': tx.platform.name, 'is_positive': is_positive
        })

    # --- 6. Общая стоимость ---
    net_worth_rub = securities_total_rub + crypto_total_rub + banking_total_rub + owed_to_me_total_rub - i_owe_total_rub

    # --- 7. Топ-5 активов по стоимости ---
    # --- Top 5 Securities ---
    securities_valued_assets = []
    for asset in securities_assets:
        value_rub = (asset.quantity or 0) * (asset.current_price or 0) * currency_rates_to_rub.get(asset.currency_of_price, Decimal(1.0))
        securities_valued_assets.append({'asset': asset, 'value_rub': value_rub})

    top_5_securities_sorted = sorted(securities_valued_assets, key=lambda x: x['value_rub'], reverse=True)[:5]
    top_securities_isins = [item['asset'].ticker for item in top_5_securities_sorted]

    securities_price_changes_raw = db.session.query(
        HistoricalPriceCache.ticker, 
        HistoricalPriceCache.period, 
        HistoricalPriceCache.change_percent
    ).filter(
        HistoricalPriceCache.ticker.in_(top_securities_isins),
        HistoricalPriceCache.period.in_(['1d', '7d', '30d'])
    ).all()

    securities_changes_by_isin = defaultdict(dict)
    for isin, period, change in securities_price_changes_raw:
        securities_changes_by_isin[isin][period] = change

    top_5_securities = []
    for item in top_5_securities_sorted:
        isin = item['asset'].ticker
        item['changes'] = securities_changes_by_isin.get(isin, {})
        top_5_securities.append(item)

    # --- Top 5 Crypto ---
    aggregated_crypto_assets = defaultdict(lambda: {
        'total_quantity': Decimal(0),
        'total_value_rub': Decimal(0),
        'name': ''
    })

    for asset in crypto_assets: 
        ticker = asset.ticker
        quantity = asset.quantity or Decimal(0)
        price = asset.current_price or Decimal(0)
        
        asset_value_usdt = quantity * price
        asset_value_rub = asset_value_usdt * currency_rates_to_rub.get('USDT', Decimal(1.0))

        agg = aggregated_crypto_assets[ticker]
        agg['total_quantity'] += quantity
        agg['total_value_rub'] += asset_value_rub
        agg['name'] = asset.name 

    top_5_crypto_sorted = sorted(aggregated_crypto_assets.items(), key=lambda item: item[1]['total_value_rub'], reverse=True)[:5]
    top_crypto_tickers = [ticker for ticker, data in top_5_crypto_sorted]

    crypto_price_changes_raw = db.session.query(
        HistoricalPriceCache.ticker, 
        HistoricalPriceCache.period, 
        HistoricalPriceCache.change_percent
    ).filter(
        HistoricalPriceCache.ticker.in_(top_crypto_tickers),
        HistoricalPriceCache.period.in_(['24h', '7d', '30d'])
    ).all()

    crypto_changes_by_ticker = defaultdict(dict)
    for ticker, period, change in crypto_price_changes_raw:
        period_key = '1d' if period == '24h' else period
        crypto_changes_by_ticker[ticker][period_key] = change

    top_5_crypto = []
    for ticker, data in top_5_crypto_sorted:
        top_5_crypto.append({
            'ticker': ticker,
            'name': data['name'],
            'value_rub': data['total_value_rub'],
            'changes': crypto_changes_by_ticker.get(ticker, {})
        })

    return render_template(
        'index.html',
        net_worth_rub=net_worth_rub,
        securities_summary={'total_rub': securities_total_rub, 'changes': securities_changes},
        crypto_summary={'total_rub': crypto_total_rub, 'changes': crypto_changes},
        banking_summary={'total_rub': banking_total_rub},
        debt_summary={'i_owe': i_owe_total_rub, 'owed_to_me': owed_to_me_total_rub},
        last_transactions=last_10_transactions,
        last_securities_txs=last_securities_txs,
        deposits_and_savings=deposits_and_savings,
        top_5_securities=top_5_securities,
        top_5_crypto=top_5_crypto
    )
