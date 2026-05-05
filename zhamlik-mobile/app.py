from flask import Flask, render_template, request, jsonify, send_from_directory
from extensions import db
from models import User, Account, BankingTransaction, Category, Debt, RecurringPayment
from sqlalchemy import func, desc
from datetime import datetime, date, timedelta
from decimal import Decimal
import os
import io

app = Flask(__name__)

# Configuration
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///zhamlik_mobile.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize extensions
db.init_app(app)

# Test user ID (для тестирования)
TEST_USER_ID = 1

# Mobile API Routes

@app.route('/api/mobile/overview')
def mobile_overview():
    """Главный экран с балансами и последними транзакциями"""
    # Получаем счета
    accounts = Account.query.filter_by(
        user_id=TEST_USER_ID,
        account_type='bank_account'
    ).all()

    total_balance = sum(acc.balance for acc in accounts)

    # Последние транзакции
    recent_transactions = BankingTransaction.query.join(Account).filter(
        Account.user_id == TEST_USER_ID
    ).order_by(BankingTransaction.date.desc()).limit(10).all()

    # Расходы за текущий месяц
    start_of_month = datetime.now().replace(day=1, hour=0, minute=0, second=0)
    monthly_expense = db.session.query(func.sum(BankingTransaction.amount)).join(Account).filter(
        Account.user_id == TEST_USER_ID,
        BankingTransaction.date >= start_of_month,
        BankingTransaction.transaction_type == 'expense'
    ).scalar() or Decimal(0)

    monthly_income = db.session.query(func.sum(BankingTransaction.amount)).join(Account).filter(
        Account.user_id == TEST_USER_ID,
        BankingTransaction.date >= start_of_month,
        BankingTransaction.transaction_type == 'income'
    ).scalar() or Decimal(0)

    return jsonify({
        'total_balance': float(total_balance),
        'monthly_income': float(monthly_income),
        'monthly_expense': float(monthly_expense),
        'accounts': [{
            'id': acc.id,
            'name': acc.name,
            'balance': float(acc.balance),
            'currency': acc.currency
        } for acc in accounts[:5]],
        'recent_transactions': [{
            'id': tx.id,
            'description': tx.description or 'Без названия',
            'amount': float(tx.amount),
            'date': tx.date.strftime('%Y-%m-%d'),
            'category': tx.category.name if tx.category else None
        } for tx in recent_transactions]
    })

@app.route('/api/mobile/accounts')
def mobile_accounts():
    """Список всех счетов"""
    accounts = Account.query.filter_by(user_id=TEST_USER_ID).all()

    return jsonify({
        'accounts': [{
            'id': acc.id,
            'name': acc.name,
            'balance': float(acc.balance),
            'currency': acc.currency,
            'type': acc.account_type,
            'bank': acc.bank.name if acc.bank else None
        } for acc in accounts]
    })

@app.route('/api/mobile/transactions')
def mobile_transactions():
    """Список транзакций с фильтрами"""
    page = request.args.get('page', 1, type=int)
    per_page = 20
    account_id = request.args.get('account_id')
    transaction_type = request.args.get('type')

    query = BankingTransaction.query.join(Account).filter(
        Account.user_id == TEST_USER_ID
    )

    if account_id:
        query = query.filter(BankingTransaction.account_id == int(account_id))

    if transaction_type:
        query = query.filter(BankingTransaction.transaction_type == transaction_type)

    transactions = query.order_by(BankingTransaction.date.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )

    return jsonify({
        'transactions': [{
            'id': tx.id,
            'description': tx.description or 'Без названия',
            'amount': float(tx.amount),
            'date': tx.date.strftime('%Y-%m-%d'),
            'type': tx.transaction_type,
            'category': tx.category.name if tx.category else None,
            'account': tx.account.name
        } for tx in transactions.items],
        'has_next': transactions.has_next,
        'has_prev': transactions.has_prev,
        'page': page,
        'total': transactions.total
    })

@app.route('/api/mobile/analytics')
def mobile_analytics():
    """Аналитика по расходам"""
    # Период для анализа
    days = request.args.get('days', 30, type=int)
    start_date = datetime.now() - timedelta(days=days)

    # Расходы по категориям
    category_spending = db.session.query(
        Category.name,
        func.sum(BankingTransaction.amount)
    ).join(Category, BankingTransaction.category_id == Category.id).join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == TEST_USER_ID,
        BankingTransaction.date >= start_date,
        BankingTransaction.transaction_type == 'expense'
    ).group_by(Category.name).order_by(func.sum(BankingTransaction.amount).desc()).all()

    # Динамика по дням
    daily_spending = db.session.query(
        func.date(BankingTransaction.date),
        func.sum(BankingTransaction.amount)
    ).join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == TEST_USER_ID,
        BankingTransaction.date >= start_date,
        BankingTransaction.transaction_type == 'expense'
    ).group_by(func.date(BankingTransaction.date)).order_by(func.date(BankingTransaction.date)).all()

    return jsonify({
        'categories': [{
            'name': cat[0],
            'amount': float(cat[1])
        } for cat in category_spending[:10]],
        'daily': [{
            'date': day[0].strftime('%Y-%m-%d'),
            'amount': float(day[1])
        } for day in daily_spending],
        'total_expense': float(sum(cat[1] for cat in category_spending))
    })

@app.route('/api/mobile/debts')
def mobile_debts():
    """Список долгов"""
    i_owe = Debt.query.filter_by(
        user_id=TEST_USER_ID,
        debt_type='i_owe',
        status='active'
    ).all()

    owed_to_me = Debt.query.filter_by(
        user_id=TEST_USER_ID,
        debt_type='owed_to_me',
        status='active'
    ).all()

    return jsonify({
        'i_owe': [{
            'id': debt.id,
            'counterparty': debt.counterparty,
            'amount': float(debt.initial_amount - debt.repaid_amount),
            'due_date': debt.due_date.strftime('%Y-%m-%d') if debt.due_date else None,
            'description': debt.description
        } for debt in i_owe],
        'owed_to_me': [{
            'id': debt.id,
            'counterparty': debt.counterparty,
            'amount': float(debt.initial_amount - debt.repaid_amount),
            'due_date': debt.due_date.strftime('%Y-%m-%d') if debt.due_date else None,
            'description': debt.description
        } for debt in owed_to_me]
    })

@app.route('/api/mobile/debts', methods=['POST'])
def mobile_create_debt():
    """Создание нового долга"""
    data = request.get_json()

    # Парсим дату, если есть
    due_date = None
    if data.get('due_date'):
        due_date = datetime.strptime(data.get('due_date'), '%Y-%m-%d').date()

    # Создаем долг
    debt = Debt(
        user_id=TEST_USER_ID,
        debt_type=data.get('debt_type', 'i_owe'),  # 'i_owe' или 'owed_to_me'
        counterparty=data.get('counterparty', ''),
        initial_amount=Decimal(str(data.get('amount', 0))),
        repaid_amount=Decimal(0),
        currency=data.get('currency', 'RUB'),
        due_date=due_date,
        description=data.get('description', ''),
        status='active'
    )

    db.session.add(debt)
    db.session.commit()

    return jsonify({
        'success': True,
        'message': 'Долг создан',
        'debt': {
            'id': debt.id,
            'counterparty': debt.counterparty,
            'amount': float(debt.initial_amount),
            'debt_type': debt.debt_type,
            'currency': debt.currency,
            'due_date': debt.due_date.strftime('%Y-%m-%d') if debt.due_date else None,
            'description': debt.description
        }
    })

@app.route('/api/mobile/recurring_payments')
def mobile_recurring_payments():
    """Список регулярных платежей"""
    payments = RecurringPayment.query.filter_by(user_id=TEST_USER_ID).all()

    today = date.today()
    upcoming = []

    for payment in payments:
        if payment.next_due_date and payment.next_due_date <= today + timedelta(days=7):
            upcoming.append({
                'id': payment.id,
                'description': payment.description,
                'amount': float(payment.amount),
                'currency': payment.currency,
                'next_due_date': payment.next_due_date.strftime('%Y-%m-%d'),
                'counterparty': payment.counterparty
            })

    return jsonify({
        'upcoming': sorted(upcoming, key=lambda x: x['next_due_date'])
    })

@app.route('/api/mobile/transactions', methods=['POST'])
def mobile_create_transaction():
    """Создание новой транзакции"""
    data = request.get_json()

    # Создаем транзакцию
    tx = BankingTransaction(
        user_id=TEST_USER_ID,
        account_id=data.get('account_id'),
        amount=Decimal(str(data.get('amount', 0))),
        transaction_type=data.get('type', 'expense'),
        description=data.get('description', ''),
        category_id=data.get('category_id'),
        date=datetime.strptime(data.get('date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')
    )

    db.session.add(tx)

    # Обновляем баланс счета
    account = Account.query.get(data.get('account_id'))
    if account:
        if tx.transaction_type == 'expense':
            account.balance -= tx.amount
        else:
            account.balance += tx.amount

    db.session.commit()

    return jsonify({
        'success': True,
        'message': 'Транзакция создана',
        'transaction': {
            'id': tx.id,
            'description': tx.description,
            'amount': float(tx.amount),
            'type': tx.transaction_type
        }
    })

@app.route('/api/mobile/accounts', methods=['POST'])
def mobile_create_account():
    """Создание нового счета"""
    data = request.get_json()

    # Создаем счет
    account = Account(
        user_id=TEST_USER_ID,
        name=data.get('name', 'Новый счет'),
        account_type=data.get('type', 'bank_account'),
        balance=Decimal(str(data.get('balance', 0))),
        currency=data.get('currency', 'RUB')
    )

    db.session.add(account)
    db.session.commit()

    return jsonify({
        'success': True,
        'message': 'Счет создан',
        'account': {
            'id': account.id,
            'name': account.name,
            'balance': float(account.balance),
            'currency': account.currency,
            'type': account.account_type
        }
    })

@app.route('/api/mobile/transactions/<int:transaction_id>', methods=['DELETE'])
def mobile_delete_transaction(transaction_id):
    """Удаление транзакции"""
    tx = BankingTransaction.query.filter_by(
        id=transaction_id,
        user_id=TEST_USER_ID
    ).first_or_404()

    # Возвращаем баланс счета
    account = Account.query.get(tx.account_id)
    if account:
        if tx.transaction_type == 'expense':
            account.balance += tx.amount
        else:
            account.balance -= tx.amount

    db.session.delete(tx)
    db.session.commit()

    return jsonify({
        'success': True,
        'message': 'Транзакция удалена'
    })

@app.route('/api/mobile/categories')
def mobile_categories():
    """Список категорий"""
    categories = Category.query.filter(
        (Category.user_id == TEST_USER_ID) | (Category.user_id.is_(None))
    ).all()

    return jsonify({
        'categories': [{
            'id': cat.id,
            'name': cat.name
        } for cat in categories]
    })

@app.route('/api/mobile/categories/seed', methods=['POST'])
def mobile_seed_categories():
    """Создает категории по умолчанию для пользователя"""

    # Проверяем, есть ли уже категории у пользователя
    existing_count = Category.query.filter_by(user_id=TEST_USER_ID).count()
    if existing_count > 0:
        return jsonify({
            'success': True,
            'message': 'Категории уже существуют',
            'created': 0
        })

    # Категории расходов по умолчанию
    expense_categories = [
        'Продукты',
        'Транспорт',
        'Жилье и коммунальные',
        'Здоровье',
        'Развлечения',
        'Покупки',
        'Кафе и рестораны',
        'Образование',
        'Связь и интернет',
        'Красота',
        'Путешествия',
        'Прочее'
    ]

    # Категории доходов по умолчанию
    income_categories = [
        'Зарплата',
        'Премия',
        'Подработка',
        'Инвестиции',
        'Подарки',
        'Продажи',
        'Прочее'
    ]

    created_count = 0

    for category_name in expense_categories:
        category = Category(
            user_id=TEST_USER_ID,
            name=category_name,
            type='expense'
        )
        db.session.add(category)
        created_count += 1

    for category_name in income_categories:
        category = Category(
            user_id=TEST_USER_ID,
            name=category_name,
            type='income'
        )
        db.session.add(category)
        created_count += 1

    db.session.commit()

    return jsonify({
        'success': True,
        'message': f'Создано {created_count} категорий',
        'created': created_count
    })

@app.route('/api/mobile/purchase/qr', methods=['POST'])
def mobile_process_qr_code():
    """Обработка QR-кода чека с распознаванием и получением данных из ФНС"""
    try:
        # Пробуем распознать QR-код с изображения
        qr_string = None

        if 'qr_code' in request.files:
            file = request.files['qr_code']
            if file.filename:
                try:
                    from PIL import Image
                    import pyzbar.pyzbar as pyzbar

                    img = Image.open(io.BytesIO(file.read()))
                    decoded_objects = pyzbar.decode(img)

                    if decoded_objects:
                        qr_string = decoded_objects[0].data.decode('utf-8')
                except ImportError:
                    return jsonify({
                        'success': False,
                        'message': 'Библиотеки распознавания не установлены. Введите данные вручную.',
                        'error': 'missing_libraries'
                    }), 200
                except Exception as e:
                    return jsonify({
                        'success': False,
                        'message': f'Ошибка распознавания QR-кода: {str(e)}'
                    }), 400

        # Если QR-код распознан или передан как строка
        if qr_string or request.form.get('qr_string'):
            qr_string = qr_string or request.form.get('qr_string')

            # Проверяем наличие токена
            from fns_client import PROVERKACHEKA_TOKEN
            if not PROVERKACHEKA_TOKEN:
                return jsonify({
                    'success': False,
                    'message': 'Сервис QR-кодов не настроен. Обратитесь к администратору.',
                    'error': 'token_not_configured'
                }), 503

            # Получаем данные чека
            from fns_client import parse_receipt_qr
            parsed_data = parse_receipt_qr(qr_string)

            if parsed_data.get('error'):
                return jsonify({
                    'success': False,
                    'message': parsed_data['error'],
                    'error': 'api_error'
                }), 400

            # Формируем ответ для мобильного приложения
            total_sum = parsed_data.get('total_sum', 0)
            merchant = parsed_data.get('merchant', '')
            items = parsed_data.get('items', [])

            # Формируем описание из товаров
            description = merchant
            if items and len(items) > 0:
                if len(items) == 1:
                    description = f"{items[0]['name']} ({merchant})"
                else:
                    description = f"Покупка в {merchant} ({len(items)} тов.)"

            return jsonify({
                'success': True,
                'message': 'QR-код распознан успешно',
                'amount': float(total_sum),
                'description': description,
                'date': parsed_data.get('date', ''),
                'merchant': merchant,
                'items': items[:5],  # Первые 5 товаров
                'total_items': len(items)
            })

        else:
            return jsonify({
                'success': False,
                'message': 'Не удалось распознать QR-код. Введите данные вручную.'
            }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка при обработке: {str(e)}'
        }), 500

# Static files
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Main routes
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5002)

# ==================== VERSIONS API ====================

@app.route('/api/mobile/versions')
def mobile_versions():
    """API для проверки версий приложений"""
    return jsonify({
        'success': True,
        'versions': {
            'banking': {
                'version': '1.2.8',
                'version_code': 8,
                'apk_url': 'http://193.29.224.20/apks/zhamlik.apk',
                'force_update': False,
                'changes': [
                    'Добавлены уведомления',
                    'Исправлены ошибки'
                ]
            },
            'crypto': {
                'version': '1.3.0',
                'version_code': 5,
                'apk_url': 'http://193.29.224.20/apks/zhamlik_crypto.apk',
                'force_update': False,
                'changes': [
                    'Добавлена поддержка фьючерсов',
                    'Исправлены ошибки API Bitget',
                    'Добавлены уведомления'
                ]
            }
        }
    })
