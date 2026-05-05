"""
Mobile API for Crypto Portfolio
Provides JSON endpoints for mobile/web crypto applications
"""
import os
import requests
from flask import jsonify, request, current_app
from flask_login import login_required, current_user
from datetime import datetime, date, timedelta
from decimal import Decimal
from collections import defaultdict

from routes import main_bp
from extensions import db
from models import InvestmentAsset, InvestmentPlatform, Transaction, CryptoPortfolioHistory
from api_clients import PRICE_TICKER_DISPATCHER, fetch_bybit_spot_tickers
from services.common import _get_currency_rates
from logic.platform_sync_logic import sync_platform_balances


def get_crypto_prices_binance(tickers):
    """
    Получает цены криптовалют через Binance public API.
    Возвращает словарь {ticker_uppercase: price_usd}
    """
    prices = {}

    try:
        # Binance API для получения всех цен
        url = "https://api.binance.com/api/v3/ticker/price"
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            all_prices = response.json()

            # Создадим mapping символа к цене
            for item in all_prices:
                symbol = item['symbol']
                price = float(item['price'])

                # Убираем суффиксы USDT, BUSD, etc.
                for suffix in ['USDT', 'BUSD', 'BNB', 'ETH', 'BTC', 'FDUSD']:
                    if symbol.endswith(suffix):
                        ticker = symbol[:-len(suffix)]
                        prices[ticker] = Decimal(str(price))
                        break

            current_app.logger.info(f"[PRICE] Получены цены для {len(prices)} тикеров из Binance")
        else:
            current_app.logger.warning(f"[PRICE] Binance API вернул код {response.status_code}")

    except Exception as e:
        current_app.logger.error(f"[PRICE] Ошибка получения цен из Binance: {e}")

    return prices


@main_bp.route('/api/mobile/crypto/platforms')
@login_required
def api_mobile_crypto_platforms():
    """
    Список всех крипто-бирж пользователя
    """
    try:
        platforms = InvestmentPlatform.query.filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == current_user.id
        ).order_by(InvestmentPlatform.name).all()

        # Получаем цены для всех активов
        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == current_user.id
        ).all()
        
        unique_tickers = list(set(a.ticker.upper() for a in assets))
        prices = {}
        if unique_tickers:
            try:
                symbols = [f"{t}USDT" for t in unique_tickers]
                price_data = fetch_bybit_spot_tickers(target_symbols=symbols)
                for item in price_data:
                    prices[item['ticker'].upper()] = float(item['price'])
            except:
                pass

        platforms_list = []
        for platform in platforms:
            # Получаем активы платформы
            assets = InvestmentAsset.query.filter(
                InvestmentAsset.platform_id == platform.id,
                InvestmentAsset.quantity > 0
            ).all()

            assets_data = []
            total_value = 0.0
            for asset in assets:
                price = prices.get(asset.ticker.upper(), 0.0)
                value = float(asset.quantity or 0) * price
                total_value += value
                assets_data.append({
                    'id': asset.id,
                    'ticker': asset.ticker.lower(),
                    'name': asset.ticker.upper(),
                    'quantity': float(asset.quantity or 0),
                    'price_usd': price,
                    'value_usd': value,
                    'source_account_type': asset.source_account_type or 'Spot'
                })

            platforms_list.append({
                'id': platform.id,
                'name': platform.name,
                'is_active': platform.is_active,
                'has_api_keys': bool(platform.api_key),
                'assets_count': len(assets),
                'total_value_usd': total_value,
                'assets': assets_data,
                'notes': platform.notes
            })

        return jsonify({'platforms': platforms_list})

    except Exception as e:
        return jsonify({'platforms': [], 'error': str(e)}), 200


@main_bp.route('/api/mobile/crypto/platforms', methods=['POST'])
@login_required
def api_mobile_create_platform():
    """
    Создание новой крипто-биржи
    """
    try:
        data = request.get_json()

        platform = InvestmentPlatform(
            user_id=current_user.id,
            name=data.get('name', ''),
            platform_type='crypto_exchange',
            api_key=data.get('api_key', ''),
            api_secret=data.get('api_secret', ''),
            passphrase=data.get('passphrase', ''),
            notes=data.get('notes', ''),
            is_active=data.get('is_active', True)
        )

        db.session.add(platform)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Биржа добавлена',
            'platform': {
                'id': platform.id,
                'name': platform.name
            }
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


@main_bp.route('/api/mobile/crypto/platforms/<int:platform_id>/sync', methods=['POST'])
@login_required
def api_mobile_sync_platform(platform_id):
    """
    Синхронизация балансов с биржи
    """
    try:
        platform = InvestmentPlatform.query.filter(
            InvestmentPlatform.id == platform_id,
            InvestmentPlatform.user_id == current_user.id
        ).first_or_404()

        # Запускаем синхронизацию
        success, message = sync_platform_balances(platform)

        if success:
            return jsonify({
                'success': True,
                'message': message or 'Синхронизация завершена'
            })
        else:
            return jsonify({
                'success': False,
                'message': message or 'Ошибка синхронизации'
            }), 400

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


@main_bp.route('/api/mobile/crypto/platforms/<int:platform_id>')
@login_required
def api_mobile_platform_detail(platform_id):
    """
    Детали биржи с активами
    """
    try:
        platform = InvestmentPlatform.query.filter(
            InvestmentPlatform.id == platform_id,
            InvestmentPlatform.user_id == current_user.id
        ).first_or_404()

        # Получаем активы
        assets = InvestmentAsset.query.filter(
            InvestmentAsset.platform_id == platform_id,
            InvestmentAsset.quantity > 0
        ).all()

        # Получаем цены
        unique_tickers = list(set(a.ticker.upper() for a in assets))
        prices = {}
        if unique_tickers:
            try:
                symbols = [f"{t}USDT" for t in unique_tickers]
                price_data = fetch_bybit_spot_tickers(target_symbols=symbols)
                for item in price_data:
                    prices[item['ticker'].upper()] = float(item['price'])
            except:
                pass

        assets_list = []
        total_value_usd = Decimal('0')

        for asset in assets:
            price_usd = Decimal(str(prices.get(asset.ticker.upper(), 0)))
            value_usd = asset.quantity * price_usd
            total_value_usd += value_usd

            assets_list.append({
                'id': asset.id,
                'ticker': asset.ticker.lower(),
                'name': asset.ticker.upper(),
                'quantity': float(asset.quantity),
                'source_account_type': asset.source_account_type,
                'price_usd': float(price_usd),
                'value_usd': float(value_usd)
            })

        return jsonify({
            'id': platform.id,
            'name': platform.name,
            'is_active': platform.is_active,
            'has_api_keys': bool(platform.api_key),
            'assets': assets_list,
            'total_value_usd': float(total_value_usd)
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 400


@main_bp.route('/api/mobile/crypto/assets', methods=['POST'])
@login_required
def api_mobile_create_asset():
    """
    Ручное добавление актива
    """
    try:
        data = request.get_json()

        asset = InvestmentAsset(
            user_id=current_user.id,
            platform_id=data.get('platform_id'),
            ticker=data.get('ticker', '').lower(),
            quantity=Decimal(str(data.get('quantity', 0))),
            source_account_type=data.get('source_account_type', 'Manual'),
            currency_of_price='USDT'
        )

        db.session.add(asset)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Актив добавлен'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


@main_bp.route('/api/mobile/crypto/overview')
@login_required
def api_mobile_crypto_overview():
    """
    Главный экран крипто-портфеля для мобильного приложения
    Возвращает: общий баланс, список активов, последние транзакции
    """
    try:
        # Получаем все крипто-активы пользователя
        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == current_user.id
        ).all()

        # Собираем уникальные тикеры и получаем цены
        unique_tickers = list(set(a.ticker.upper() for a in assets))
        prices = {}
        if unique_tickers:
            try:
                symbols = [f"{t}USDT" for t in unique_tickers]
                price_data = fetch_bybit_spot_tickers(target_symbols=symbols)
                for item in price_data:
                    ticker = item['ticker'].upper()
                    prices[ticker] = {
                        'price_usd': float(item['price']),
                        'name': ticker
                    }
                current_app.logger.info(f"Получены цены для {len(prices)} тикеров")
            except Exception as e:
                current_app.logger.error(f"Ошибка получения цен: {e}")

        # Агрегируем по тикеру
        aggregated = defaultdict(lambda: {
            'quantity': Decimal('0'),
            'value_usd': Decimal('0'),
            'name': '',
            'ticker': ''
        })

        total_usd = Decimal('0')

        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')

            # Получаем цену из кэша
            try:
                price_info = prices.get(ticker)
                if price_info:
                    price_usd = Decimal(str(price_info.get('price_usd', 0)))
                    value_usd = quantity * price_usd

                    aggregated[ticker]['quantity'] += quantity
                    aggregated[ticker]['value_usd'] += value_usd
                    aggregated[ticker]['name'] = price_info.get('name', ticker)
                    aggregated[ticker]['ticker'] = ticker

                    total_usd += value_usd
            except Exception as e:
                continue

        # Конвертируем в RUB
        currency_rates = _get_currency_rates()
        usd_to_rub = Decimal(str(currency_rates.get('USD', 1)))
        total_rub = total_usd * usd_to_rub

        # Формируем список активов
        assets_list = []
        for ticker, data in sorted(aggregated.items(), key=lambda x: x[1]['value_usd'], reverse=True):
            assets_list.append({
                'ticker': data['ticker'].lower(),
                'name': data['name'],
                'quantity': float(data['quantity']),
                'value_usd': float(data['value_usd'])
            })

        # Получаем последние транзакции
        recent_tx = Transaction.query.join(InvestmentPlatform).filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == current_user.id
        ).order_by(Transaction.timestamp.desc()).limit(5).all()

        transactions_list = []
        for tx in recent_tx:
            transactions_list.append({
                'id': tx.id,
                'type': tx.type,
                'asset1_ticker': tx.asset1_ticker.lower() if tx.asset1_ticker else None,
                'amount1': float(tx.asset1_amount) if tx.asset1_amount else 0,
                'timestamp': tx.timestamp.isoformat() if tx.timestamp else None
            })

        return jsonify({
            'total_balance_usd': float(total_usd),
            'total_balance_rub': float(total_rub),
            'assets': assets_list,
            'recent_transactions': transactions_list
        })

    except Exception as e:
        return jsonify({
            'total_balance_usd': 0,
            'total_balance_rub': 0,
            'assets': [],
            'recent_transactions': [],
            'error': str(e)
        }), 200


@main_bp.route('/api/mobile/crypto/assets')
@login_required
def api_mobile_crypto_assets():
    """
    Список всех крипто-активов с ценами
    """
    try:
        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == current_user.id
        ).all()

        aggregated = defaultdict(lambda: {
            'quantity': Decimal('0'),
            'value_usd': Decimal('0'),
            'name': '',
            'ticker': '',
            'price_usd': Decimal('0')
        })

        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')

            try:
                price_info = PRICE_TICKER_DISPATCHER.get(ticker.lower())
                if price_info:
                    price_usd = Decimal(str(price_info.get('price_usd', 0)))
                    value_usd = quantity * price_usd

                    aggregated[ticker]['quantity'] += quantity
                    aggregated[ticker]['value_usd'] += value_usd
                    aggregated[ticker]['price_usd'] = price_usd
                    aggregated[ticker]['name'] = price_info.get('name', ticker)
                    aggregated[ticker]['ticker'] = ticker
            except Exception as e:
                continue

        assets_list = []
        for ticker, data in sorted(aggregated.items(), key=lambda x: x[1]['value_usd'], reverse=True):
            assets_list.append({
                'ticker': data['ticker'].lower(),
                'name': data['name'],
                'quantity': float(data['quantity']),
                'value_usd': float(data['value_usd']),
                'price_usd': float(data['price_usd'])
            })

        return jsonify({'assets': assets_list})

    except Exception as e:
        return jsonify({'assets': [], 'error': str(e)}), 200


@main_bp.route('/api/mobile/crypto/transactions')
@login_required
def api_mobile_crypto_transactions():
    """
    Список крипто-транзакций с пагинацией
    """
    try:
        page = request.args.get('page', 1, type=int)
        per_page = 20
        filter_type = request.args.get('filter_type', 'all')

        query = Transaction.query.join(InvestmentPlatform).filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == current_user.id
        )

        if filter_type != 'all':
            query = query.filter(Transaction.type == filter_type)

        transactions = query.order_by(Transaction.timestamp.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )

        transactions_list = []
        for tx in transactions.items:
            transactions_list.append({
                'id': tx.id,
                'type': tx.type,
                'asset1_ticker': tx.asset1_ticker.lower() if tx.asset1_ticker else None,
                'amount1': float(tx.asset1_amount) if tx.asset1_amount else 0,
                'asset2_ticker': tx.asset2_ticker.lower() if tx.asset2_ticker else None,
                'amount2': float(tx.asset2_amount) if tx.asset2_amount else 0,
                'timestamp': tx.timestamp.isoformat() if tx.timestamp else None,
                'platform_name': tx.platform.name if tx.platform else None
            })

        return jsonify({
            'transactions': transactions_list,
            'has_next': transactions.has_next,
            'total': transactions.total
        })

    except Exception as e:
        return jsonify({'transactions': [], 'error': str(e)}), 200


@main_bp.route('/api/mobile/crypto/analytics')
@login_required
def api_mobile_crypto_analytics():
    """
    Аналитика по крипто-портфелю
    """
    try:
        days = request.args.get('days', 30, type=int)
        start_date = date.today() - timedelta(days=days)

        # Получаем распределение портфеля
        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == current_user.id
        ).all()

        aggregated = defaultdict(lambda: {'value_usd': Decimal('0')})

        total_value = Decimal('0')
        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')

            try:
                price_info = PRICE_TICKER_DISPATCHER.get(ticker.lower())
                if price_info:
                    price_usd = Decimal(str(price_info.get('price_usd', 0)))
                    value_usd = quantity * price_usd
                    aggregated[ticker]['value_usd'] += value_usd
                    total_value += value_usd
            except Exception as e:
                continue

        # Распределение портфеля в процентах
        distribution = []
        for ticker, data in sorted(aggregated.items(), key=lambda x: x[1]['value_usd'], reverse=True):
            percentage = (data['value_usd'] / total_value * 100) if total_value > 0 else 0
            distribution.append({
                'ticker': ticker.upper(),
                'percentage': float(percentage),
                'value_usd': float(data['value_usd'])
            })

        # История портфеля
        history = CryptoPortfolioHistory.query.filter(
            CryptoPortfolioHistory.date >= start_date
        ).order_by(CryptoPortfolioHistory.date.asc()).all()

        performance_chart = []
        for h in history:
            performance_chart.append({
                'date': h.date.strftime('%Y-%m-%d'),
                'value_usd': float(h.total_value_ust)
            })

        return jsonify({
            'portfolio_distribution': distribution,
            'performance_chart': performance_chart
        })

    except Exception as e:
        return jsonify({
            'portfolio_distribution': [],
            'performance_chart': [],
            'error': str(e)
        }), 200


@main_bp.route('/api/mobile/crypto/prices')
@login_required
def api_mobile_crypto_prices():
    """
    Текущие цены криптовалют из портфеля пользователя
    """
    try:
        # Получаем все уникальные тикеры из активов пользователя
        tickers = db.session.query(InvestmentAsset.ticker).join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentPlatform.user_id == current_user.id
        ).distinct().all()

        prices = []
        for (ticker,) in tickers:
            try:
                price_info = PRICE_TICKER_DISPATCHER.get(ticker.lower())
                if price_info:
                    prices.append({
                        'symbol': ticker.lower(),
                        'name': price_info.get('name', ticker),
                        'price_usd': str(price_info.get('price_usd', 0)),
                        'price_change_24h': str(price_info.get('price_change_24h', 0))
                    })
            except Exception as e:
                continue

        return jsonify({'prices': prices})

    except Exception as e:
        return jsonify({'prices': [], 'error': str(e)}), 200


# Internal API endpoints (без @login_required) для crypto app
# Используют shared secret для авторизации между сервисами
SHARED_SECRET = os.environ.get('INTERNAL_API_SECRET', 'zhamlik-internal-secret-2024')


def check_internal_auth():
    """Проверяет internal authorization"""
    # Проверяем header
    auth_header = request.headers.get('X-Internal-Auth')
    if auth_header == SHARED_SECRET:
        return True

    # Проверяем параметр
    if request.args.get('internal_secret') == SHARED_SECRET:
        return True

    return False


@main_bp.route('/api/internal/crypto/overview')
def api_internal_crypto_overview():
    """
    Internal endpoint для crypto app без @login_required
    Требует X-Internal-Auth header с shared secret
    """
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        # Получаем ID первого пользователя (обычно это admin)
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id

        # Получаем все крипто-активы пользователя
        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == user_id
        ).all()

        # Агрегируем по тикеру
        aggregated = defaultdict(lambda: {
            'quantity': Decimal('0'),
            'value_usd': Decimal('0'),
            'ticker': ''
        })

        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')
            aggregated[ticker]['quantity'] += quantity
            aggregated[ticker]['ticker'] = ticker

        # Получаем цены из Binance
        tickers = list(aggregated.keys())
        prices = get_crypto_prices_binance(tickers)

        total_usd = Decimal('0')

        # Вычисляем стоимость
        for ticker, data in aggregated.items():
            quantity = data['quantity']
            price_usd = prices.get(ticker, Decimal('0'))

            # Stablecoins имеют цену 1
            if ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD', 'TUSD']:
                price_usd = Decimal('1')

            value_usd = quantity * price_usd
            data['value_usd'] = value_usd
            total_usd += value_usd

        # Формируем список активов
        assets_list = []
        for ticker, data in sorted(aggregated.items(), key=lambda x: x[1]['value_usd'], reverse=True):
            assets_list.append({
                'ticker': data['ticker'].lower(),
                'name': data['ticker'],  # TODO: Можно добавить названия из CoinGecko
                'quantity': float(data['quantity']),
                'value_usd': float(data['value_usd'])
            })

        # Конвертируем в RUB
        currency_rates = _get_currency_rates()
        usd_to_rub = Decimal(str(currency_rates.get('USD', 1)))
        total_rub = total_usd * usd_to_rub

        # Получаем последние транзакции
        recent_tx = Transaction.query.join(InvestmentPlatform).filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == user_id
        ).order_by(Transaction.timestamp.desc()).limit(5).all()

        transactions_list = []
        for tx in recent_tx:
            transactions_list.append({
                'id': tx.id,
                'type': tx.type,
                'asset1_ticker': tx.asset1_ticker.lower() if tx.asset1_ticker else None,
                'amount1': float(tx.asset1_amount) if tx.asset1_amount else 0,
                'timestamp': tx.timestamp.isoformat() if tx.timestamp else None
            })

        return jsonify({
            'total_balance_usd': float(total_usd),
            'total_balance_rub': float(total_rub),
            'assets': assets_list,
            'recent_transactions': transactions_list
        })

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Error: {e}", exc_info=True)
        return jsonify({
            'total_balance_usd': 0,
            'total_balance_rub': 0,
            'assets': [],
            'recent_transactions': [],
            'error': str(e)
        }), 200


@main_bp.route('/api/internal/crypto/platforms')
def api_internal_crypto_platforms():
    """Internal endpoint для списка бирж"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id

        platforms = InvestmentPlatform.query.filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == user_id
        ).order_by(InvestmentPlatform.name).all()

        platforms_list = []
        for platform in platforms:
            assets_count = InvestmentAsset.query.filter(
                InvestmentAsset.platform_id == platform.id,
                InvestmentAsset.quantity > 0
            ).count()

            platforms_list.append({
                'id': platform.id,
                'name': platform.name,
                'is_active': platform.is_active,
                'has_api_keys': bool(platform.api_key),
                'assets_count': assets_count,
                'notes': platform.notes
            })

        return jsonify({'platforms': platforms_list})

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Platforms Error: {e}", exc_info=True)
        return jsonify({'platforms': [], 'error': str(e)}), 200


@main_bp.route('/api/internal/crypto/platforms/<int:platform_id>')
def api_internal_platform_detail(platform_id):
    """Internal endpoint для деталей биржи"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id

        platform = InvestmentPlatform.query.filter(
            InvestmentPlatform.id == platform_id,
            InvestmentPlatform.user_id == user_id
        ).first()

        if not platform:
            return jsonify({'error': 'Platform not found'}), 404

        # Получаем активы
        assets = InvestmentAsset.query.filter(
            InvestmentAsset.platform_id == platform_id,
            InvestmentAsset.quantity > 0
        ).all()

        assets_list = []
        total_value_usd = Decimal('0')

        # Получаем цены
        tickers = [asset.ticker.upper() for asset in assets]
        prices = get_crypto_prices_binance(tickers)

        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')

            price_usd = prices.get(ticker, Decimal('0'))
            if ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD', 'TUSD']:
                price_usd = Decimal('1')

            value_usd = quantity * price_usd
            total_value_usd += value_usd

            assets_list.append({
                'id': asset.id,
                'ticker': ticker.lower(),
                'name': ticker,
                'quantity': float(quantity),
                'source_account_type': asset.source_account_type,
                'price_usd': float(price_usd),
                'value_usd': float(value_usd)
            })

        return jsonify({
            'id': platform.id,
            'name': platform.name,
            'is_active': platform.is_active,
            'has_api_keys': bool(platform.api_key),
            'assets': assets_list,
            'total_value_usd': float(total_value_usd)
        })

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Platform Detail Error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@main_bp.route('/api/internal/crypto/transactions')
def api_internal_crypto_transactions():
    """Internal endpoint для списка транзакций"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id
        page = request.args.get('page', 1, type=int)
        per_page = 20
        filter_type = request.args.get('filter_type', 'all')

        query = Transaction.query.join(InvestmentPlatform).filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == user_id
        )

        if filter_type != 'all':
            query = query.filter(Transaction.type == filter_type)

        transactions = query.order_by(Transaction.timestamp.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )

        transactions_list = []
        for tx in transactions.items:
            transactions_list.append({
                'id': tx.id,
                'type': tx.type,
                'asset1_ticker': tx.asset1_ticker.lower() if tx.asset1_ticker else None,
                'amount1': float(tx.asset1_amount) if tx.asset1_amount else 0,
                'asset2_ticker': tx.asset2_ticker.lower() if tx.asset2_ticker else None,
                'amount2': float(tx.asset2_amount) if tx.asset2_amount else 0,
                'timestamp': tx.timestamp.isoformat() if tx.timestamp else None,
                'platform_name': tx.platform.name if tx.platform else None
            })

        return jsonify({
            'transactions': transactions_list,
            'has_next': transactions.has_next,
            'total': transactions.total
        })

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Transactions Error: {e}", exc_info=True)
        return jsonify({'transactions': [], 'error': str(e)}), 200


@main_bp.route('/api/internal/crypto/analytics')
def api_internal_crypto_analytics():
    """Internal endpoint для аналитики"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id
        days = request.args.get('days', 30, type=int)
        start_date = date.today() - timedelta(days=days)

        # Получаем распределение портфеля
        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == user_id
        ).all()

        # Используем Binance для получения цен
        tickers = list(set([asset.ticker.upper() for asset in assets]))
        prices = get_crypto_prices_binance(tickers)

        aggregated = defaultdict(lambda: {'value_usd': Decimal('0')})
        total_value = Decimal('0')

        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')

            # Stablecoins имеют цену 1
            if ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD', 'TUSD']:
                price_usd = Decimal('1')
            else:
                price_usd = prices.get(ticker, Decimal('0'))

            value_usd = quantity * price_usd
            aggregated[ticker]['value_usd'] += value_usd
            total_value += value_usd

        # Распределение портфеля в процентах
        distribution = []
        for ticker, data in sorted(aggregated.items(), key=lambda x: x[1]['value_usd'], reverse=True):
            percentage = (data['value_usd'] / total_value * 100) if total_value > 0 else 0
            distribution.append({
                'ticker': ticker.lower(),
                'percentage': float(percentage),
                'value_usd': float(data['value_usd'])
            })

        # История портфеля
        history = CryptoPortfolioHistory.query.filter(
            CryptoPortfolioHistory.date >= start_date
        ).order_by(CryptoPortfolioHistory.date.asc()).all()

        performance_chart = []
        for h in history:
            performance_chart.append({
                'date': h.date.strftime('%Y-%m-%d'),
                'value_usd': float(h.total_value_ust)
            })

        return jsonify({
            'portfolio_distribution': distribution,
            'performance_chart': performance_chart
        })

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Analytics Error: {e}", exc_info=True)
        return jsonify({
            'portfolio_distribution': [],
            'performance_chart': [],
            'error': str(e)
        }), 200


@main_bp.route('/api/internal/crypto/prices')
def api_internal_crypto_prices():
    """Internal endpoint для цен криптовалют"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id

        # Получаем все уникальные тикеры из активов пользователя
        tickers = db.session.query(InvestmentAsset.ticker).join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentPlatform.user_id == user_id
        ).distinct().all()

        ticker_list = [ticker for (ticker,) in tickers]
        prices_dict = get_crypto_prices_binance(ticker_list)

        prices = []
        for ticker in ticker_list:
            ticker_upper = ticker.upper()
            price_usd = prices_dict.get(ticker_upper)

            if price_usd is not None:
                prices.append({
                    'symbol': ticker.lower(),
                    'name': ticker_upper,  # TODO: Можно добавить названия из CoinGecko
                    'price_usd': str(price_usd),
                    'price_change_24h': '0'  # Binance public API не предоставляет 24h change в ticker/price
                })

        return jsonify({'prices': prices})

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Prices Error: {e}", exc_info=True)
        return jsonify({'prices': [], 'error': str(e)}), 200


@main_bp.route('/api/internal/crypto/platforms', methods=['POST'])
def api_internal_create_platform():
    """Internal endpoint для создания биржи"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        data = request.get_json()

        platform = InvestmentPlatform(
            user_id=user.id,
            name=data.get('name', ''),
            platform_type='crypto_exchange',
            api_key=data.get('api_key', ''),
            api_secret=data.get('api_secret', ''),
            passphrase=data.get('passphrase', ''),
            notes=data.get('notes', ''),
            is_active=data.get('is_active', True)
        )

        db.session.add(platform)
        db.session.commit()

        current_app.logger.info(f"[INTERNAL_API] Created platform: {platform.name} (ID: {platform.id})")

        return jsonify({
            'success': True,
            'message': 'Биржа добавлена',
            'platform': {
                'id': platform.id,
                'name': platform.name
            }
        })

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[INTERNAL_API] Create Platform Error: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


@main_bp.route('/api/internal/crypto/platforms/<int:platform_id>/sync', methods=['POST'])
def api_internal_sync_platform(platform_id):
    """Internal endpoint для синхронизации биржи"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        platform = InvestmentPlatform.query.filter(
            InvestmentPlatform.id == platform_id,
            InvestmentPlatform.user_id == user.id
        ).first()

        if not platform:
            return jsonify({
                'success': False,
                'message': 'Биржа не найдена'
            }), 404

        # Запускаем синхронизацию
        success, message = sync_platform_balances(platform)

        if success:
            current_app.logger.info(f"[INTERNAL_API] Synced platform: {platform.name} - {message}")
            return jsonify({
                'success': True,
                'message': message or 'Синхронизация завершена'
            })
        else:
            current_app.logger.warning(f"[INTERNAL_API] Sync failed for {platform.name}: {message}")
            return jsonify({
                'success': False,
                'message': message or 'Ошибка синхронизации'
            }), 400

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Sync Platform Error: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


@main_bp.route('/api/internal/crypto/assets')
def api_internal_crypto_assets():
    """Internal endpoint для списка всех крипто-активов"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        user_id = user.id

        assets = InvestmentAsset.query.join(InvestmentPlatform).filter(
            InvestmentAsset.asset_type == 'crypto',
            InvestmentAsset.quantity > 0,
            InvestmentPlatform.user_id == user_id
        ).all()

        # Агрегируем по тикерам (одинаковые активы с разных бирж суммируем)
        aggregated = defaultdict(lambda: {
            'quantity': Decimal('0'),
            'value_usd': Decimal('0'),
            'name': '',
            'ticker': '',
            'price_usd': Decimal('0')
        })

        # Собираем все тикеры для получения цен
        tickers = list(set([asset.ticker.upper() for asset in assets]))
        prices = get_crypto_prices_binance(tickers)

        for asset in assets:
            ticker = asset.ticker.upper()
            quantity = asset.quantity or Decimal('0')

            # Stablecoins имеют цену 1
            if ticker in ['USDT', 'USDC', 'DAI', 'BUSD', 'FDUSD', 'TUSD']:
                price_usd = Decimal('1')
            else:
                price_usd = prices.get(ticker, Decimal('0'))

            value_usd = quantity * price_usd

            aggregated[ticker]['quantity'] += quantity
            aggregated[ticker]['value_usd'] += value_usd
            aggregated[ticker]['name'] = ticker
            aggregated[ticker]['ticker'] = ticker
            aggregated[ticker]['price_usd'] = price_usd

        # Формируем список активов
        assets_list = []
        for ticker, data in sorted(aggregated.items(), key=lambda x: x[1]['value_usd'], reverse=True):
            if data['quantity'] > 0:  # Только активы с положительным балансом
                assets_list.append({
                    'ticker': data['ticker'].lower(),
                    'name': data['name'],
                    'quantity': float(data['quantity']),
                    'price_usd': float(data['price_usd']),
                    'value_usd': float(data['value_usd'])
                })

        return jsonify({'assets': assets_list})

    except Exception as e:
        current_app.logger.error(f"[INTERNAL_API] Assets Error: {e}", exc_info=True)
        return jsonify({'assets': [], 'error': str(e)}), 200


@main_bp.route('/api/internal/crypto/assets', methods=['POST'])
def api_internal_create_asset():
    """Internal endpoint для создания актива"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        data = request.get_json()

        # Проверяем, что платформа принадлежит пользователю
        platform = InvestmentPlatform.query.filter(
            InvestmentPlatform.id == data.get('platform_id'),
            InvestmentPlatform.user_id == user.id
        ).first()

        if not platform:
            return jsonify({
                'success': False,
                'message': 'Биржа не найдена'
            }), 404

        asset = InvestmentAsset(
            ticker=data.get('ticker', '').upper(),
            name=data.get('ticker', '').upper(),
            asset_type='crypto',
            quantity=Decimal(str(data.get('quantity', 0))),
            platform_id=platform.id,
            source_account_type='Manual'
        )

        db.session.add(asset)
        db.session.commit()

        current_app.logger.info(f"[INTERNAL_API] Created asset: {asset.ticker} on {platform.name}")

        return jsonify({
            'success': True,
            'message': 'Актив добавлен',
            'asset': {
                'id': asset.id,
                'ticker': asset.ticker
            }
        })

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[INTERNAL_API] Create Asset Error: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


@main_bp.route('/api/internal/crypto/platforms/<int:platform_id>', methods=['DELETE'])
def api_internal_delete_platform(platform_id):
    """Internal endpoint для удаления биржи"""
    if not check_internal_auth():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from models import User
        user = User.query.first()
        if not user:
            return jsonify({'error': 'No users found'}), 404

        platform = InvestmentPlatform.query.filter(
            InvestmentPlatform.id == platform_id,
            InvestmentPlatform.user_id == user.id
        ).first()

        if not platform:
            return jsonify({
                'success': False,
                'message': 'Биржа не найдена'
            }), 404

        platform_name = platform.name

        # Удаляем связанные активы
        InvestmentAsset.query.filter(InvestmentAsset.platform_id == platform_id).delete()

        # Удаляем платформу
        db.session.delete(platform)
        db.session.commit()

        current_app.logger.info(f"[INTERNAL_API] Deleted platform: {platform_name} (ID: {platform_id})")

        return jsonify({
            'success': True,
            'message': 'Биржа удалена'
        })

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[INTERNAL_API] Delete Platform Error: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 400


# ==================== FUTURES API ====================

@main_bp.route('/api/mobile/crypto/futures/overview')
@login_required
def api_mobile_crypto_futures_overview():
    """Общий обзор фьючерсных позиций"""
    try:
        from models import InvestmentPlatform, InvestmentAsset
        
        platforms = InvestmentPlatform.query.filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == current_user.id
        ).all()
        
        total_unrealized_pnl = 0.0
        total_positions = 0
        platforms_with_positions = 0
        
        for platform in platforms:
            has_positions = False
            assets = InvestmentAsset.query.filter(
                InvestmentAsset.platform_id == platform.id,
                InvestmentAsset.asset_type == 'futures'
            ).all()
            
            for asset in assets:
                if hasattr(asset, 'unrealized_pnl') and asset.unrealized_pnl:
                    total_unrealized_pnl += float(asset.unrealized_pnl)
                    total_positions += 1
                    has_positions = True
            
            if has_positions:
                platforms_with_positions += 1
        
        return jsonify({
            'success': True,
            'total_unrealized_pnl': total_unrealized_pnl,
            'total_positions': total_positions,
            'platforms_with_positions': platforms_with_positions,
            'total_platforms': len(platforms)
        })
    
    except Exception as e:
        current_app.logger.error(f"[MOBILE_API] Futures Overview Error: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@main_bp.route('/api/mobile/crypto/futures/positions')
@login_required
def api_mobile_crypto_futures_positions():
    """Список фьючерсных позиций"""
    try:
        from models import InvestmentPlatform, InvestmentAsset
        
        platforms = InvestmentPlatform.query.filter(
            InvestmentPlatform.platform_type == 'crypto_exchange',
            InvestmentPlatform.user_id == current_user.id
        ).all()
        
        positions = []
        for platform in platforms:
            assets = InvestmentAsset.query.filter(
                InvestmentAsset.platform_id == platform.id,
                InvestmentAsset.asset_type == 'futures',
                InvestmentAsset.quantity > 0
            ).all()
            
            for asset in assets:
                positions.append({
                    'platform_id': platform.id,
                    'platform_name': platform.name,
                    'symbol': asset.ticker,
                    'side': getattr(asset, 'side', 'long'),
                    'size': float(asset.quantity),
                    'entry_price': float(getattr(asset, 'entry_price', 0) or 0),
                    'mark_price': float(getattr(asset, 'mark_price', 0) or 0),
                    'unrealized_pnl': float(getattr(asset, 'unrealized_pnl', 0) or 0),
                    'fees': float(getattr(asset, 'fees', 0) or 0),
                    'leverage': getattr(asset, 'leverage', 1) or 1
                })
        
        return jsonify({
            'success': True,
            'positions': positions
        })
    
    except Exception as e:
        current_app.logger.error(f"[MOBILE_API] Futures Positions Error: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'positions': []
        }), 500
