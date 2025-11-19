import json
from datetime import datetime, date, timezone, timedelta
from collections import defaultdict, namedtuple
from decimal import Decimal, InvalidOperation
from flask import render_template, request, redirect, url_for, flash, current_app, jsonify
from sqlalchemy.orm import joinedload
from sqlalchemy import func, asc, desc, or_
from flask_login import login_required, current_user

from routes import main_bp
from extensions import db
from models import InvestmentPlatform, InvestmentAsset, Transaction, HistoricalPriceCache, CryptoPortfolioHistory
from api_clients import PRICE_TICKER_DISPATCHER
from services.common import _get_currency_rates
from analytics_logic import get_performance_chart_data_from_cache, refresh_crypto_price_change_data, refresh_performance_chart_data, refresh_crypto_portfolio_history
from news_logic import get_crypto_news, get_securities_news
from logic.news_analysis import get_news_trends_for_portfolio
# Import platform sync logic if needed, or import the function from main_routes if it's moved to a service

# Helper functions related to investments
def _get_sync_function(platform_name: str, dispatcher: dict):
    """
    Вспомогательная функция для поиска функции синхронизации в диспетчере.
    Поддерживает "нечеткий" поиск для распространенных имен бирж.
    """
    name_lower = platform_name.lower().replace('-', '').replace(' ', '')
    
    sync_function = dispatcher.get(name_lower)
    if sync_function:
        return sync_function
        
    alias_map = {
        'kucoin': ['kukoin'],
        'bybit': [], 'bingx': [], 'bitget': [], 'okx': []
    }
    for canonical, aliases in alias_map.items():
        if canonical in name_lower or any(alias in name_lower for alias in aliases):
            return dispatcher.get(canonical)
            
    return None

def _apply_crypto_transaction_filters_and_sort(query, args):
    """
    Применяет общие фильтры и сортировку из аргументов запроса к запросу транзакций.
    """
    filter_type = args.get('filter_type', 'all')
    if filter_type != 'all':
        if filter_type == 'buy_sell':
            query = query.filter(Transaction.type.in_(['buy', 'sell']))
        else:
            query = query.filter(Transaction.type == filter_type)
    
    if args.get('filter_platform_id', 'all') != 'all':
        query = query.filter(Transaction.platform_id == int(args.get('filter_platform_id')))

    if args.get('filter_asset', 'all') != 'all':
        query = query.filter(
            or_(Transaction.asset1_ticker == args.get('filter_asset'), Transaction.asset2_ticker == args.get('filter_asset')))
    
    sort_by = args.get('sort_by', 'timestamp')
    order = args.get('order', 'desc')
    sort_column = getattr(Transaction, sort_by, Transaction.timestamp)
    if order == 'desc':
        query = query.order_by(desc(sort_column))
    else:
        query = query.order_by(asc(sort_column))
        
    return query

# Routes
@main_bp.route('/platforms')
@login_required
def ui_investment_platforms():
    platforms = InvestmentPlatform.query.filter_by(platform_type='crypto_exchange', user_id=current_user.id).order_by(InvestmentPlatform.name).all()
    return render_template('investment_platforms.html', platforms=platforms)

@main_bp.route('/platforms/add', methods=['GET', 'POST'])
@login_required
def ui_add_investment_platform_form():
    if request.method == 'POST':
        manual_earn_balances_input = request.form.get('manual_earn_balances_input', '{}')
        try:
            json.loads(manual_earn_balances_input)
        except json.JSONDecodeError:
            flash('Неверный формат JSON для ручных Earn балансов. Используйте {"TICKER": "QUANTITY"}.', 'danger')
            current_data = request.form.to_dict()
            return render_template('add_investment_platform.html', current_data=current_data)

        new_platform = InvestmentPlatform(
            name=request.form['name'],
            platform_type=request.form['platform_type'],
            api_key=request.form.get('api_key'),
            api_secret=request.form.get('api_secret'),
            passphrase=request.form.get('passphrase'),
            other_credentials_json=request.form.get('other_credentials_json'),
            notes=request.form.get('notes'),
            is_active='is_active' in request.form,
            manual_earn_balances_json=manual_earn_balances_input,
            user_id=current_user.id
        )
        db.session.add(new_platform)
        db.session.commit()
        flash(f'Платформа "{new_platform.name}" успешно добавлена.', 'success')
        return redirect(url_for('main.ui_investment_platforms'))
    return render_template('add_investment_platform.html', current_data={})

@main_bp.route('/platforms/<int:platform_id>')
@login_required
def ui_investment_platform_detail(platform_id):
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    currency_rates_to_rub = _get_currency_rates()

    all_valued_assets = []
    platform_total_value_rub = Decimal(0)
    platform_total_value_usdt = Decimal(0)
    account_type_summary = defaultdict(lambda: {'rub': Decimal(0), 'usdt': Decimal(0)})
    assets_with_balance = platform.assets.filter(InvestmentAsset.quantity > 0).order_by(InvestmentAsset.source_account_type, InvestmentAsset.ticker)
    for asset in assets_with_balance:
        quantity = asset.quantity or Decimal(0)
        price = asset.current_price or Decimal(0)
        rate = currency_rates_to_rub.get(asset.currency_of_price, Decimal(1.0))

        asset_value_usdt = quantity * price
        platform_total_value_usdt += asset_value_usdt

        asset_value_rub = asset_value_usdt * rate
        platform_total_value_rub += asset_value_rub
        
        account_type = asset.source_account_type or 'Unknown'
        account_type_summary[account_type]['rub'] += asset_value_rub
        account_type_summary[account_type]['usdt'] += asset_value_usdt
            
        all_valued_assets.append({'asset': asset, 'value_rub': asset_value_rub, 'value_usdt': asset_value_usdt})

    manual_earn_balances = {}
    try:
        manual_earn_balances = json.loads(platform.manual_earn_balances_json)
    except (json.JSONDecodeError, TypeError):
        flash('Ошибка чтения ручных Earn балансов (неверный JSON). Пожалуйста, исправьте.', 'danger')
        manual_earn_balances = {}

    manual_tickers_to_fetch = [t for t, q_str in manual_earn_balances.items() if Decimal(q_str) > 0 and t.upper() not in ['USDT', 'USDC', 'DAI']]
    manual_prices = {}
    price_fetcher_config = PRICE_TICKER_DISPATCHER.get(platform.name.lower())
    if price_fetcher_config and manual_tickers_to_fetch:
        try:
            symbols_for_api = [f"{ticker}{price_fetcher_config['suffix']}" for ticker in manual_tickers_to_fetch]
            ticker_data_list = price_fetcher_config['func'](target_symbols=symbols_for_api)
            for item in ticker_data_list:
                manual_prices[item['ticker']] = Decimal(item['price'])
        except Exception as e:
            current_app.logger.error(f"Ошибка получения цен для ручных Earn балансов: {e}")

    for ticker, quantity_str in manual_earn_balances.items():
        try:
            quantity = Decimal(quantity_str)
            if quantity <= 0: continue

            currency_of_price = 'USDT'
            current_price = manual_prices.get(ticker)

            if current_price is None:
                current_price = Decimal('1.0') if ticker.upper() in ['USDT', 'USDC', 'DAI'] else Decimal('0.0')

            asset_value_usdt = quantity * (current_price or Decimal(0))
            platform_total_value_usdt += asset_value_usdt
            asset_value_rub = asset_value_usdt * currency_rates_to_rub.get(currency_of_price, Decimal(1.0))
            platform_total_value_rub += asset_value_rub
            
            account_type_summary['Manual Earn']['rub'] += asset_value_rub
            account_type_summary['Manual Earn']['usdt'] += asset_value_usdt

            DummyAsset = namedtuple('InvestmentAsset', ['ticker', 'name', 'quantity', 'current_price', 'currency_of_price', 'source_account_type', 'id'])
            all_valued_assets.append({
                'asset': DummyAsset(
                    ticker=ticker, name=f"{ticker} (Ручной Earn)", quantity=quantity,
                    current_price=current_price, currency_of_price=currency_of_price,
                    source_account_type='Manual Earn', id=None
                ),
                'value_rub': asset_value_rub, 'value_usdt': asset_value_usdt
            })
        except InvalidOperation:
            flash(f'Неверное количество для {ticker} в ручных Earn балансах. Проверьте формат.', 'danger')
        except Exception as e:
            print(f"Непредвиденная ошибка при обработке ручного Earn баланса для {ticker}: {e}")
            flash(f'Непредвиденная ошибка при обработке ручного Earn баланса для {ticker}: {e}', 'danger')

    all_valued_assets.sort(key=lambda x: (x['asset'].source_account_type or '', x['asset'].ticker or ''))
    sorted_account_type_summary = sorted(account_type_summary.items(), key=lambda item: item[0])
    
    page = request.args.get('page', 1, type=int)
    sort_by = request.args.get('sort_by', 'timestamp')
    order = request.args.get('order', 'desc')
    filter_type = request.args.get('filter_type', 'all')

    transactions_query = platform.transactions

    if filter_type != 'all':
        transactions_query = transactions_query.filter_by(type=filter_type)

    sort_column = getattr(Transaction, sort_by, Transaction.timestamp)
    transactions_query = transactions_query.order_by(desc(sort_column) if order == 'desc' else asc(sort_column))
    
    transactions_pagination = transactions_query.paginate(page=page, per_page=15, error_out=False)
    platform_transactions = transactions_pagination.items

    unique_transaction_types = [t.type for t in platform.transactions.with_entities(Transaction.type).distinct().all()]
    unique_transaction_types.sort()

    return render_template(
        'investment_platform_detail.html', 
        platform=platform, valued_assets=all_valued_assets, platform_total_value_rub=platform_total_value_rub,
        platform_total_value_usdt=platform_total_value_usdt, account_type_summary=sorted_account_type_summary, 
        platform_transactions=platform_transactions, transactions_pagination=transactions_pagination,
        sort_by=sort_by, order=order, filter_type=filter_type, unique_transaction_types=unique_transaction_types
    )

@main_bp.route('/platforms/<int:platform_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_investment_platform_form(platform_id):
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    if request.method == 'POST':
        manual_earn_balances_input = request.form.get('manual_earn_balances_input', '{}')
        try:
            json.loads(manual_earn_balances_input)
        except json.JSONDecodeError:
            flash('Неверный формат JSON для ручных Earn балансов. Используйте {"TICKER": "QUANTITY"}.', 'danger')
            platform.manual_earn_balances_json = manual_earn_balances_input
            return render_template('edit_investment_platform.html', platform=platform)

        platform.name = request.form['name']
        platform.platform_type = request.form['platform_type']
        platform.api_key = request.form.get('api_key')
        
        if request.form.get('api_secret'):
            platform.api_secret = request.form.get('api_secret')
        if request.form.get('passphrase'):
            platform.passphrase = request.form.get('passphrase')
        if request.form.get('other_credentials_json'):
            platform.other_credentials_json = request.form.get('other_credentials_json')
        platform.notes = request.form.get('notes')
        platform.is_active = 'is_active' in request.form
        platform.manual_earn_balances_json = manual_earn_balances_input
        
        db.session.commit()
        flash(f'Данные платформы "{platform.name}" успешно обновлены.', 'success')
        return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform.id))
    return render_template('edit_investment_platform.html', platform=platform)

@main_bp.route('/platforms/<int:platform_id>/sync', methods=['POST'])
@login_required
def ui_sync_investment_platform(platform_id):
    from logic.platform_sync_logic import sync_platform_balances # Import here to avoid circular dependency
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    success, message = sync_platform_balances(platform)
    if success:
        flash(f'Синхронизация балансов для "{platform.name}" завершена. {message}', 'success')
    else:
        flash(f'Ошибка при синхронизации балансов для "{platform.name}": {message}', 'danger')
    return redirect(request.referrer or url_for('main.ui_investment_platform_detail', platform_id=platform.id))

@main_bp.route('/platforms/<int:platform_id>/sync_transactions', methods=['POST'])
@login_required
def ui_sync_investment_platform_transactions(platform_id):
    from logic.platform_sync_logic import sync_platform_transactions # Import here to avoid circular dependency
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    success, message = sync_platform_transactions(platform)
    if success:
        flash(f'Синхронизация транзакций для "{platform.name}" завершена. {message}', 'success')
    else:
        flash(f'Ошибка при синхронизации транзакций для "{platform.name}": {message}', 'danger')
    return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform.id))

@main_bp.route('/platforms/<int:platform_id>/delete', methods=['POST'])
@login_required
def ui_delete_investment_platform(platform_id):
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    platform_name = platform.name
    db.session.delete(platform)
    db.session.commit()
    flash(f'Платформа "{platform_name}" и все связанные с ней данные были удалены.', 'success')
    return redirect(url_for('main.ui_investment_platforms'))

@main_bp.route('/platforms/<int:platform_id>/assets/add', methods=['GET', 'POST'])
@login_required
def ui_add_investment_asset_form(platform_id):
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    if platform.platform_type != 'crypto_exchange':
        flash('Добавление активов вручную поддерживается только для крипто-платформ.', 'warning')
        return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform.id))

    if request.method == 'POST':
        try:
            ticker = request.form.get('ticker', '').upper().strip()
            quantity_str = request.form.get('quantity', '0').replace(',', '.')
            source_account_type = request.form.get('source_account_type', 'Manual').strip()

            if not ticker:
                raise ValueError('Тикер является обязательным полем.')
            if not source_account_type:
                raise ValueError('Тип кошелька является обязательным полем.')

            quantity = Decimal(quantity_str)
            if quantity <= 0:
                raise ValueError('Количество должно быть положительным числом.')

            existing_asset = InvestmentAsset.query.filter_by(
                platform_id=platform.id,
                ticker=ticker,
                source_account_type=source_account_type
            ).first()

            if existing_asset:
                existing_asset.quantity += quantity
                db.session.commit()
                flash(f'К существующему активу {ticker} ({source_account_type}) добавлено {quantity}.', 'success')
            else:
                current_price = Decimal('0')
                currency_of_price = 'USDT'
                if ticker.upper() in ['USDT', 'USDC', 'DAI']:
                    current_price = Decimal('1.0')
                else:
                    price_fetcher_config = _get_sync_function(platform.name, PRICE_TICKER_DISPATCHER)
                    if price_fetcher_config:
                        try:
                            api_symbol = f"{ticker}{price_fetcher_config['suffix']}"
                            ticker_data_list = price_fetcher_config['func'](target_symbols=[api_symbol])
                            if ticker_data_list:
                                current_price = Decimal(ticker_data_list[0]['price'])
                                flash(f'Цена для {ticker} была автоматически получена: {current_price} USDT.', 'info')
                        except Exception as e:
                            current_app.logger.warning(f"Не удалось получить цену для {ticker} при ручном добавлении: {e}")
                            flash(f'Не удалось автоматически получить цену для {ticker}.', 'warning')
                
                new_asset = InvestmentAsset(platform_id=platform.id, ticker=ticker, name=ticker, asset_type='crypto', quantity=quantity, current_price=current_price, currency_of_price=currency_of_price, source_account_type=source_account_type)
                db.session.add(new_asset)
                db.session.commit()
                flash(f'Актив {ticker} ({quantity}) успешно добавлен в кошелек {source_account_type}.', 'success')

            return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform.id))
        except (ValueError, InvalidOperation) as e:
            db.session.rollback()
            flash(f'Ошибка в данных: {e}', 'danger')
            return render_template('add_crypto_asset.html', platform=platform, current_data=request.form)
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Ошибка при добавлении крипто-актива: {e}", exc_info=True)
            flash(f'Произошла непредвиденная ошибка: {e}', 'danger')
            return render_template('add_crypto_asset.html', platform=platform, current_data=request.form)

    return render_template('add_crypto_asset.html', platform=platform, current_data={})

@main_bp.route('/crypto-assets/<int:asset_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_investment_asset(asset_id):
    asset = InvestmentAsset.query.get_or_404(asset_id)
    # Check ownership via platform
    if asset.platform.user_id != current_user.id:
         return current_app.login_manager.unauthorized()
         
    manual_types = ['Manual', 'Manual Earn', 'Staking', 'Lending']
    if asset.source_account_type not in manual_types:
        flash(f'Редактирование актива {asset.ticker} ({asset.source_account_type}) запрещено, так как он синхронизируется автоматически.', 'warning')
        return redirect(url_for('main.ui_investment_platform_detail', platform_id=asset.platform_id))

    if request.method == 'POST':
        try:
            quantity_str = request.form.get('quantity', '0').replace(',', '.')
            source_account_type = request.form.get('source_account_type', '').strip()

            if not source_account_type:
                raise ValueError('Тип кошелька является обязательным полем.')

            quantity = Decimal(quantity_str)
            if quantity < 0:
                raise ValueError('Количество не может быть отрицательным.')

            asset.quantity = quantity
            asset.source_account_type = source_account_type
            
            db.session.commit()
            flash(f'Актив {asset.ticker} успешно обновлен.', 'success')
            return redirect(url_for('main.ui_investment_platform_detail', platform_id=asset.platform_id))

        except (ValueError, InvalidOperation) as e:
            db.session.rollback()
            flash(f'Ошибка в данных: {e}', 'danger')
            return render_template('edit_crypto_asset.html', asset=asset, current_data=request.form)
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Ошибка при редактировании крипто-актива: {e}", exc_info=True)
            flash(f'Произошла непредвиденная ошибка: {e}', 'danger')
            return render_template('edit_crypto_asset.html', asset=asset, current_data=request.form)

    return render_template('edit_crypto_asset.html', asset=asset, current_data=asset)

@main_bp.route('/crypto-assets/<int:asset_id>/delete', methods=['POST'])
@login_required
def ui_delete_investment_asset(asset_id):
    asset = InvestmentAsset.query.get_or_404(asset_id)
    if asset.platform.user_id != current_user.id:
         return current_app.login_manager.unauthorized()
         
    platform_id = asset.platform_id
    
    manual_types = ['Manual', 'Manual Earn', 'Staking', 'Lending']
    if asset.source_account_type not in manual_types:
        flash(f'Удаление актива {asset.ticker} ({asset.source_account_type}) запрещено, так как он синхронизируется автоматически.', 'warning')
        return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform_id))
    
    try:
        asset_ticker = asset.ticker
        db.session.delete(asset)
        db.session.commit()
        flash(f'Актив "{asset_ticker}" успешно удален.', 'success')
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Ошибка при удалении крипто-актива: {e}", exc_info=True)
        flash(f'Произошла ошибка при удалении актива: {e}', 'danger')
        
    return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform_id))

@main_bp.route('/platforms/<int:platform_id>/transactions/add_exchange', methods=['GET', 'POST'])
@login_required
def ui_add_exchange_transaction_form(platform_id):
    platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=current_user.id).first_or_404()
    asset_tickers = {asset.ticker for asset in platform.assets.filter(InvestmentAsset.quantity > 0).all()}
    try:
        manual_balances = json.loads(platform.manual_earn_balances_json)
        asset_tickers.update(manual_balances.keys())
    except (json.JSONDecodeError, TypeError):
        pass
    asset_tickers.update(['USDT', 'USDC', 'BTC', 'ETH'])
    available_assets = sorted(list(asset_tickers))

    if request.method == 'POST':
        try:
            asset1_ticker = request.form.get('asset1_ticker')
            asset1_amount = Decimal(request.form.get('asset1_amount'))
            asset2_ticker = request.form.get('asset2_ticker')
            asset2_amount = Decimal(request.form.get('asset2_amount'))
            fee_amount = Decimal(request.form.get('fee_amount', '0'))
            fee_currency = request.form.get('fee_currency')
            timestamp = datetime.strptime(request.form.get('timestamp'), '%Y-%m-%dT%H:%M').replace(tzinfo=timezone.utc)

            if not all([asset1_ticker, asset1_amount, asset2_ticker, asset2_amount]):
                raise ValueError("Все поля активов и их количества обязательны.")
            if asset1_ticker == asset2_ticker:
                raise ValueError("Активы для обмена должны быть разными.")

            new_tx = Transaction(
                platform_id=platform.id, timestamp=timestamp, type='exchange', raw_type='Manual Exchange',
                asset1_ticker=asset1_ticker, asset1_amount=asset1_amount,
                asset2_ticker=asset2_ticker, asset2_amount=asset2_amount,
                fee_amount=fee_amount, fee_currency=fee_currency if fee_amount > 0 else None,
                description=request.form.get('description'),
                user_id=current_user.id
            )
            db.session.add(new_tx)
            db.session.commit()
            flash('Транзакция обмена успешно добавлена.', 'success')
            return redirect(url_for('main.ui_investment_platform_detail', platform_id=platform.id))
        except (ValueError, InvalidOperation) as e:
            db.session.rollback()
            flash(f'Ошибка в данных: {e}', 'danger')
    
    return render_template('add_exchange_transaction.html', platform=platform, available_assets=available_assets, now=datetime.now(timezone.utc), cancel_url=url_for('main.ui_investment_platform_detail', platform_id=platform.id))

@main_bp.route('/crypto-assets')
@login_required
def ui_crypto_assets():
    all_crypto_assets = InvestmentAsset.query.join(InvestmentPlatform).options(joinedload(InvestmentAsset.platform)).filter(
        InvestmentAsset.asset_type == 'crypto', InvestmentAsset.quantity > 0, InvestmentPlatform.user_id == current_user.id).all()
    currency_rates_to_rub = _get_currency_rates()

    if not all_crypto_assets:
        return render_template('crypto_assets_overview.html', assets=[], grand_total_rub=0, grand_total_usdt=0, platform_summary=[], chart_labels='[]', chart_data='[]', chart_history_labels='[]', chart_history_values='[]')

    aggregated_assets = defaultdict(lambda: {
        'total_quantity': Decimal(0),
        'total_value_rub': Decimal(0),
        'total_value_usdt': Decimal(0),
        'locations': [],
        'current_price': Decimal(0),
        'currency_of_price': 'USDT',
        'average_buy_price': Decimal(0)
    })

    platform_summary_agg = defaultdict(lambda: {'total_rub': Decimal(0), 'total_usdt': Decimal(0), 'id': None})
    grand_total_rub = Decimal(0)
    grand_total_usdt = Decimal(0)

    for asset in all_crypto_assets:
        ticker = asset.ticker
        quantity = asset.quantity or Decimal(0)
        price = asset.current_price or Decimal(0)
        
        asset_value_usdt = quantity * price
        asset_value_rub = asset_value_usdt * currency_rates_to_rub.get('USDT', Decimal(1.0))

        agg = aggregated_assets[ticker]
        agg['total_quantity'] += quantity
        agg['total_value_usdt'] += asset_value_usdt
        agg['total_value_rub'] += asset_value_rub
        agg['current_price'] = price
        agg['currency_of_price'] = asset.currency_of_price or 'USDT'
        agg['locations'].append({
            'platform_name': asset.platform.name,
            'platform_id': asset.platform_id,
            'account_type': asset.source_account_type,
            'quantity': quantity
        })

        plat_summary = platform_summary_agg[asset.platform.name]
        plat_summary['id'] = asset.platform_id
        plat_summary['total_rub'] += asset_value_rub
        plat_summary['total_usdt'] += asset_value_usdt

        grand_total_rub += asset_value_rub
        grand_total_usdt += asset_value_usdt

    all_tickers = list(aggregated_assets.keys())
    
    price_changes = db.session.query(HistoricalPriceCache.ticker, HistoricalPriceCache.period, HistoricalPriceCache.change_percent).filter(HistoricalPriceCache.ticker.in_(all_tickers)).all()
    changes_by_ticker = defaultdict(lambda: {
        '24h': None, '7d': None, '30d': None, '90d': None, '180d': None, '365d': None
    })

    for ticker, period, change in price_changes:
        changes_by_ticker[ticker][period] = change

    buy_transactions = db.session.query(
        Transaction.asset1_ticker,
        func.sum(Transaction.asset2_amount).label('total_cost_usdt'),
        func.sum(Transaction.asset1_amount).label('total_quantity_bought')
    ).join(InvestmentPlatform).filter(
        Transaction.type == 'buy',
        Transaction.asset1_ticker.in_(all_tickers),
        Transaction.asset2_ticker == 'USDT',
        InvestmentPlatform.user_id == current_user.id
    ).group_by(Transaction.asset1_ticker).all()

    avg_buy_prices = {}
    for ticker, total_cost, total_quantity in buy_transactions:
        cost = total_cost if total_cost is not None else Decimal(0)
        qty = total_quantity if total_quantity is not None else Decimal(0)
        if qty > 0:
            avg_buy_prices[ticker] = cost / qty
        else:
            avg_buy_prices[ticker] = Decimal(0)

    for ticker, data in aggregated_assets.items():
        data.update(changes_by_ticker[ticker])
        data['average_buy_price'] = avg_buy_prices.get(ticker, Decimal(0))

    final_assets_list = sorted(aggregated_assets.items(), key=lambda item: item[1]['total_value_rub'], reverse=True)
    platform_summary = sorted(platform_summary_agg.items(), key=lambda item: item[1]['total_rub'], reverse=True)

    chart_labels = [item[0] for item in final_assets_list]
    chart_data = [float(item[1]['total_value_rub']) for item in final_assets_list]

    history_data = CryptoPortfolioHistory.query.order_by(CryptoPortfolioHistory.date.asc()).all()
    chart_history_labels = [h.date.strftime('%Y-%m-%d') for h in history_data]
    chart_history_values = [float(h.total_value_rub) for h in history_data]

    assets_with_pnl = []
    for ticker, data in final_assets_list:
        if data['average_buy_price'] > 0:
            invested_usdt = data['total_quantity'] * data['average_buy_price']
            pnl_usdt = data['total_value_usdt'] - invested_usdt
            assets_with_pnl.append({'ticker': ticker, 'pnl': pnl_usdt})
    
    sorted_pnl = sorted(assets_with_pnl, key=lambda x: x['pnl'], reverse=True)
    pnl_chart_labels = [item['ticker'] for item in sorted_pnl]
    pnl_chart_data = [float(item['pnl']) for item in sorted_pnl]

    platform_pie_labels = [item[0] for item in platform_summary]
    platform_pie_data = [float(item[1]['total_rub']) for item in platform_summary]

    performance_chart_data, performance_chart_last_updated = get_performance_chart_data_from_cache()

    return render_template('crypto_assets_overview.html',
                           assets=final_assets_list, grand_total_rub=grand_total_rub, grand_total_usdt=grand_total_usdt,
                           platform_summary=platform_summary, chart_labels=json.dumps(chart_labels),
                           chart_data=json.dumps(chart_data), chart_history_labels=json.dumps(chart_history_labels),
                           chart_history_values=json.dumps(chart_history_values),
                           performance_chart_data=json.dumps(performance_chart_data),
                           performance_chart_last_updated=performance_chart_last_updated,
                           pnl_chart_labels=json.dumps(pnl_chart_labels),
                           pnl_chart_data=json.dumps(pnl_chart_data),
                           platform_pie_labels=json.dumps(platform_pie_labels),
                           platform_pie_data=json.dumps(platform_pie_data))

@main_bp.route('/api/crypto-transactions')
@login_required
def api_crypto_transactions():
    page = request.args.get('page', 1, type=int)
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')

    transactions_query = Transaction.query.join(InvestmentPlatform).filter(
        InvestmentPlatform.platform_type == 'crypto_exchange',
        InvestmentPlatform.user_id == current_user.id
    ).options(joinedload(Transaction.platform))

    transactions_query = _apply_crypto_transaction_filters_and_sort(transactions_query, request.args)

    try:
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            transactions_query = transactions_query.filter(Transaction.timestamp >= start_date)
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            transactions_query = transactions_query.filter(Transaction.timestamp < end_date + timedelta(days=1))
    except ValueError:
        pass 

    pagination = transactions_query.paginate(page=page, per_page=150, error_out=False)
    transactions = pagination.items

    html = render_template('_crypto_transaction_rows.html', transactions=transactions)
    return jsonify({'html': html, 'has_next': pagination.has_next})

@main_bp.route('/crypto-transactions')
@login_required
def ui_crypto_transactions():
    page = request.args.get('page', 1, type=int)
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')

    transactions_query = Transaction.query.join(InvestmentPlatform).filter(
        InvestmentPlatform.platform_type == 'crypto_exchange',
        InvestmentPlatform.user_id == current_user.id
    ).options(joinedload(Transaction.platform))

    transactions_query = _apply_crypto_transaction_filters_and_sort(transactions_query, request.args)

    try:
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            transactions_query = transactions_query.filter(Transaction.timestamp >= start_date)
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            transactions_query = transactions_query.filter(Transaction.timestamp < end_date + timedelta(days=1))
    except ValueError:
        flash('Неверный формат даты. Используйте ГГГГ-ММ-ДД.', 'danger')
        start_date_str, end_date_str = '', '' 

    transactions_pagination = transactions_query.paginate(page=page, per_page=150, error_out=False)
    
    unique_transaction_types = [r[0] for r in db.session.query(Transaction.type).join(InvestmentPlatform).filter(InvestmentPlatform.platform_type == 'crypto_exchange', InvestmentPlatform.user_id == current_user.id).distinct().order_by(Transaction.type).all()]
    available_platforms = InvestmentPlatform.query.filter_by(platform_type='crypto_exchange', user_id=current_user.id).order_by(InvestmentPlatform.name).all()
    
    asset1_tickers = db.session.query(Transaction.asset1_ticker).join(InvestmentPlatform).filter(
        InvestmentPlatform.platform_type == 'crypto_exchange',
        Transaction.asset1_ticker.isnot(None),
        InvestmentPlatform.user_id == current_user.id
    ).distinct()
    asset2_tickers = db.session.query(Transaction.asset2_ticker).join(InvestmentPlatform).filter(
        InvestmentPlatform.platform_type == 'crypto_exchange',
        Transaction.asset2_ticker.isnot(None),
        InvestmentPlatform.user_id == current_user.id
    ).distinct()
    unique_assets = sorted(list(set([r[0] for r in asset1_tickers] + [r[0] for r in asset2_tickers])))

    return render_template('crypto_transactions.html', 
                           transactions=transactions_pagination.items,
                           pagination=transactions_pagination,
                           sort_by=request.args.get('sort_by', 'timestamp'), 
                           order=request.args.get('order', 'desc'), 
                           filter_type=request.args.get('filter_type', 'all'), 
                           filter_platform_id=request.args.get('filter_platform_id', 'all'),
                           filter_asset=request.args.get('filter_asset', 'all'),
                           start_date=start_date_str,
                           end_date=end_date_str,
                           unique_transaction_types=unique_transaction_types, 
                           platforms=available_platforms, 
                           unique_assets=unique_assets)

@main_bp.route('/crypto-assets/refresh-historical-data', methods=['POST'])
@login_required
def ui_refresh_historical_data():
    success, message = refresh_crypto_price_change_data()
    if success:
        flash(message, 'success')
    else:
        flash(message, 'danger')
    return redirect(url_for('main.ui_crypto_assets'))

@main_bp.route('/analytics/refresh-performance-chart', methods=['POST'])
@login_required
def ui_refresh_performance_chart():
    flash('Началось обновление данных для графика производительности. Пожалуйста, подождите...', 'info')
    success, message = refresh_performance_chart_data()
    if success:
        flash(message, 'success')
    else:
        flash(message, 'danger')
    return redirect(url_for('main.ui_crypto_assets'))

@main_bp.route('/analytics/refresh-portfolio-history', methods=['POST'])
@login_required
def ui_refresh_portfolio_history():
    flash('Началось обновление истории портфеля. Это может занять несколько минут...', 'info')
    success, message = refresh_crypto_portfolio_history()
    if success:
        flash(message, 'success')
    else:
        flash(message, 'danger')
    return redirect(url_for('main.ui_crypto_assets'))

@main_bp.route('/crypto-news')
def ui_crypto_news():
    news_trends, top_10_tickers = get_news_trends_for_portfolio()
    try:
        latest_news = get_crypto_news(limit=30)
    except Exception as e:
        current_app.logger.error(f"Не удалось загрузить общие новости: {e}")
        latest_news = []
        flash('Не удалось загрузить последние новости.', 'danger')

    return render_template(
        'crypto_news.html',
        latest_news=latest_news,
        news_trends=news_trends,
        top_10_tickers=top_10_tickers
    )

@main_bp.route('/securities-news')
def ui_securities_news():
    try:
        news_articles = get_securities_news(limit=50)
    except Exception as e:
        current_app.logger.error(f"Не удалось загрузить новости фондового рынка: {e}")
        flash("Не удалось загрузить новости. Попробуйте позже.", "danger")
        news_articles = []
    return render_template('securities_news.html', news_articles=news_articles)
