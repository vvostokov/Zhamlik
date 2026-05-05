"""
Mobile API Routes for Zhamlik Flutter Application
RESTful API endpoints for mobile client
"""
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user, login_user
from werkzeug.security import check_password_hash
from datetime import datetime, date, timedelta
from decimal import Decimal
from sqlalchemy.orm import joinedload
import io
from PIL import Image
import pyzbar.pyzbar as pyzbar

from extensions import db
from models import (
    User, Account, BankingTransaction, Category, Debt,
    InvestmentPlatform, InvestmentAsset, Transaction,
    RecurringPayment, Notification
)
from fns_client import parse_receipt_qr

mobile_bp = Blueprint('mobile_api', __name__, url_prefix='/api/mobile')


# ==================== VERSIONS ====================

@mobile_bp.route('/versions', methods=['GET'])
def get_versions():
    """Get app versions for mobile"""
    return jsonify({
        'success': True,
        'versions': {
            'banking': {
                'version': '1.2.8',
                'version_code': 8,
                'release_date': '2026-04-14',
                'apk_url': 'http://193.29.224.20/apks/zhamlik.apk',
                'changes': ['Исправлены ошибки'],
                'force_update': False
            },
            'crypto': {
                'version': '1.3.0',
                'version_code': 5,
                'release_date': '2026-04-14',
                'apk_url': 'http://193.29.224.20/apks/zhamlik_crypto.apk',
                'changes': ['Исправлены ошибки API', 'Добавлены фьючерсы'],
                'force_update': False
            }
        }
    })


# ==================== AUTH ====================

@mobile_bp.route('/auth/login', methods=['POST'])
def mobile_login():
    """Login endpoint for mobile app"""
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    user = User.query.filter_by(username=username).first()

    if user and user.check_password(password):
        login_user(user)
        return jsonify({
            'success': True,
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email
            }
        })

    return jsonify({'error': 'Invalid credentials'}), 401


@mobile_bp.route('/auth/me', methods=['GET'])
@login_required
def get_current_user():
    """Get current user info"""
    return jsonify({
        'id': current_user.id,
        'username': current_user.username,
        'email': current_user.email
    })


# ==================== ACCOUNTS ====================

@mobile_bp.route('/accounts', methods=['GET'])
@login_required
def get_accounts():
    """Get all user accounts"""
    accounts = Account.query.filter_by(user_id=current_user.id).all()

    accounts_data = []
    for acc in accounts:
        accounts_data.append({
            'id': acc.id,
            'name': acc.name,
            'type': acc.account_type,
            'currency': acc.currency,
            'balance': float(acc.balance),
            'is_active': acc.is_active,
            'notes': acc.notes
        })

    return jsonify({'accounts': accounts_data})


@mobile_bp.route('/accounts/<int:account_id>', methods=['GET'])
@login_required
def get_account(account_id):
    """Get specific account details"""
    account = Account.query.filter_by(id=account_id, user_id=current_user.id).first()

    if not account:
        return jsonify({'error': 'Account not found'}), 404

    return jsonify({
        'id': account.id,
        'name': account.name,
        'type': account.account_type,
        'currency': account.currency,
        'balance': float(account.balance),
        'is_active': account.is_active,
        'notes': account.notes
    })


# ==================== TRANSACTIONS ====================

@mobile_bp.route('/transactions', methods=['GET'])
@login_required
def get_transactions():
    """Get user transactions with pagination and filters"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    transaction_type = request.args.get('type')  # 'income', 'expense', 'transfer'
    account_id = request.args.get('account_id', type=int)

    # Build query - use explicit join on account_id
    query = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id
    )

    if transaction_type:
        query = query.filter(BankingTransaction.transaction_type == transaction_type)

    if account_id:
        query = query.filter(BankingTransaction.account_id == account_id)

    # Order by date descending
    query = query.order_by(BankingTransaction.date.desc())

    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    transactions_data = []
    for tx in pagination.items:
        transactions_data.append({
            'id': tx.id,
            'amount': float(tx.amount),
            'to_amount': float(tx.to_amount) if tx.to_amount else None,
            'type': tx.transaction_type,
            'date': tx.date.isoformat(),
            'description': tx.description,
            'merchant': tx.merchant,
            'counterparty': tx.counterparty,
            'account_id': tx.account_id,
            'to_account_id': tx.to_account_id,
            'category_id': tx.category_id,
            'debt_id': tx.debt_id
        })

    return jsonify({
        'transactions': transactions_data,
        'pagination': {
            'page': page,
            'pages': pagination.pages,
            'per_page': per_page,
            'total': pagination.total
        }
    })


@mobile_bp.route('/transactions', methods=['POST'])
@login_required
def create_transaction():
    """Create a new transaction"""
    data = request.get_json()

    # Validate required fields
    required_fields = ['amount', 'transaction_type', 'account_id']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    # Verify account belongs to user
    account = Account.query.filter_by(id=data['account_id'], user_id=current_user.id).first()
    if not account:
        return jsonify({'error': 'Account not found'}), 404

    # Create transaction
    transaction = BankingTransaction(
        amount=Decimal(str(data['amount'])),
        transaction_type=data['transaction_type'],
        account_id=data['account_id'],
        date=datetime.fromisoformat(data['date']) if data.get('date') else datetime.now(),
        description=data.get('description'),
        merchant=data.get('merchant'),
        counterparty=data.get('counterparty'),
        to_account_id=data.get('to_account_id'),
        category_id=data.get('category_id'),
        debt_id=data.get('debt_id'),
        user_id=current_user.id
    )

    db.session.add(transaction)

    # Update account balance
    if data['transaction_type'] == 'income':
        account.balance += Decimal(str(data['amount']))
    elif data['transaction_type'] == 'expense':
        account.balance -= Decimal(str(data['amount']))
    elif data['transaction_type'] == 'transfer' and data.get('to_account_id'):
        to_account = Account.query.filter_by(id=data['to_account_id'], user_id=current_user.id).first()
        if to_account:
            to_account.balance += Decimal(str(data['amount']))

    db.session.commit()

    return jsonify({
        'success': True,
        'transaction': {
            'id': transaction.id,
            'amount': float(transaction.amount),
            'type': transaction.transaction_type,
            'date': transaction.date.isoformat()
        }
    }), 201


@mobile_bp.route('/transactions/<int:tx_id>', methods=['GET'])
@login_required
def get_transaction(tx_id):
    """Get specific transaction details"""
    transaction = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(
        BankingTransaction.id == tx_id,
        Account.user_id == current_user.id
    ).first()

    if not transaction:
        return jsonify({'error': 'Transaction not found'}), 404

    return jsonify({
        'id': transaction.id,
        'amount': float(transaction.amount),
        'to_amount': float(transaction.to_amount) if transaction.to_amount else None,
        'type': transaction.transaction_type,
        'date': transaction.date.isoformat(),
        'description': transaction.description,
        'merchant': transaction.merchant,
        'counterparty': transaction.counterparty,
        'account_id': transaction.account_id,
        'to_account_id': transaction.to_account_id,
        'category_id': transaction.category_id,
        'debt_id': transaction.debt_id,
        'items': [
            {
                'id': item.id,
                'name': item.name,
                'quantity': float(item.quantity),
                'price': float(item.price),
                'total': float(item.total)
            }
            for item in transaction.items
        ] if transaction.items else []
    })


# ==================== CATEGORIES ====================

@mobile_bp.route('/categories', methods=['GET'])
@login_required
def get_categories():
    """Get all user categories"""
    categories = Category.query.filter_by(user_id=current_user.id).all()

    categories_data = []
    for cat in categories:
        categories_data.append({
            'id': cat.id,
            'name': cat.name,
            'type': cat.type,
            'parent_id': cat.parent_id
        })

    return jsonify({'categories': categories_data})


# ==================== OVERVIEW ====================

@mobile_bp.route('/overview', methods=['GET'])
@login_required
def get_overview():
    """Get main dashboard overview"""
    from services.common import _get_currency_rates

    currency_rates = _get_currency_rates()

    # Get accounts
    accounts = Account.query.filter_by(user_id=current_user.id).all()

    total_balance = Decimal(0)
    accounts_summary = []

    for acc in accounts:
        balance_rub = acc.balance * currency_rates.get(acc.currency, Decimal(1))
        total_balance += balance_rub

        accounts_summary.append({
            'id': acc.id,
            'name': acc.name,
            'type': acc.account_type,
            'currency': acc.currency,
            'balance': float(acc.balance),
            'balance_rub': float(balance_rub)
        })

    # Get recent transactions
    recent_txs = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id
    ).order_by(BankingTransaction.date.desc()).limit(10).all()

    transactions_summary = []
    for tx in recent_txs:
        transactions_summary.append({
            'id': tx.id,
            'amount': float(tx.amount),
            'type': tx.transaction_type,
            'date': tx.date.isoformat(),
            'description': tx.description or '',
            'account_name': tx.account_ref.name
        })

    # Calculate income/expense for this month
    month_start = date.today().replace(day=1)
    monthly_txs = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id,
        BankingTransaction.date >= month_start
    ).all()

    monthly_income = sum(tx.amount for tx in monthly_txs if tx.transaction_type == 'income')
    monthly_expense = sum(tx.amount for tx in monthly_txs if tx.transaction_type == 'expense')

    return jsonify({
        'total_balance': float(total_balance),
        'monthly_income': float(monthly_income),
        'monthly_expense': float(monthly_expense),
        'accounts': accounts_summary,
        'recent_transactions': transactions_summary
    })


# ==================== QR SCANNER ====================

@mobile_bp.route('/parse-qr', methods=['POST'])
@login_required
def handle_parse_qr():
    """
    Parse QR code from receipt
    Accepts JSON with 'qr_string' or image file
    """
    qr_string = None

    # Check for JSON data
    if request.is_json:
        data = request.get_json()
        qr_string = data.get('qr_string')
    # Check for form data
    elif 'qr_string' in request.form:
        qr_string = request.form['qr_string']
    # Check for image upload
    elif 'qr_image' in request.files:
        try:
            img = Image.open(io.BytesIO(request.files['qr_image'].read()))
            decoded_objects = pyzbar.decode(img)
            if decoded_objects:
                qr_string = decoded_objects[0].data.decode('utf-8')
        except Exception as e:
            return jsonify({'error': f'Ошибка обработки изображения: {str(e)}'}), 400

    if not qr_string:
        return jsonify({'error': 'Необходимо передать qr_string или изображение'}), 400

    # Check FNS token
    from fns_client import PROVERKACHEKA_TOKEN
    if not PROVERKACHEKA_TOKEN:
        return jsonify({'error': 'Сервис QR-кодов не настроен'}), 503

    try:
        parsed_data = parse_receipt_qr(qr_string)

        if parsed_data.get('error'):
            return jsonify(parsed_data), 400

        return jsonify(parsed_data), 200

    except Exception as e:
        return jsonify({'error': f'Внутренняя ошибка: {str(e)}'}), 500


@mobile_bp.route('/receipt-to-transaction', methods=['POST'])
@login_required
def receipt_to_transaction():
    """
    Convert parsed receipt data to transaction
    Expected JSON: {
        'receipt_data': { ...parsed receipt... },
        'account_id': int,
        'category_id': int (optional)
    }
    """
    data = request.get_json()
    receipt_data = data.get('receipt_data')
    account_id = data.get('account_id')
    category_id = data.get('category_id')

    if not receipt_data or not account_id:
        return jsonify({'error': 'Missing receipt_data or account_id'}), 400

    # Verify account
    account = Account.query.filter_by(id=account_id, user_id=current_user.id).first()
    if not account:
        return jsonify({'error': 'Account not found'}), 404

    try:
        # Create main transaction
        total_amount = Decimal(str(receipt_data.get('total_sum', 0)))
        transaction = BankingTransaction(
            amount=total_amount,
            transaction_type='expense',
            account_id=account_id,
            date=datetime.now(),
            description=f"Чек от {receipt_data.get('date', '')}",
            merchant=receipt_data.get('merchant', ''),
            category_id=category_id,
            user_id=current_user.id
        )

        db.session.add(transaction)
        db.session.flush()  # Get transaction ID

        # Add items if present
        items = receipt_data.get('items', [])
        for item in items:
            from models import TransactionItem
            tx_item = TransactionItem(
                name=item.get('name', ''),
                quantity=Decimal(str(item.get('quantity', 1))),
                price=Decimal(str(item.get('price', 0))),
                total=Decimal(str(item.get('sum', 0))),
                transaction_id=transaction.id,
                category_id=category_id
            )
            db.session.add(tx_item)

        # Update account balance
        account.balance -= total_amount

        db.session.commit()

        return jsonify({
            'success': True,
            'transaction': {
                'id': transaction.id,
                'amount': float(transaction.amount),
                'description': transaction.description
            }
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Error creating transaction: {str(e)}'}), 500


# ==================== DEBTS ====================

@mobile_bp.route('/debts', methods=['GET'])
@login_required
def get_debts():
    """Get all user debts"""
    debts = Debt.query.filter_by(user_id=current_user.id).all()

    i_owe = []
    owed_to_me = []

    for debt in debts:
        debt_data = {
            'id': debt.id,
            'counterparty': debt.counterparty,
            'initial_amount': float(debt.initial_amount),
            'repaid_amount': float(debt.repaid_amount),
            'remaining': float(debt.initial_amount - debt.repaid_amount),
            'currency': debt.currency,
            'status': debt.status,
            'due_date': debt.due_date.isoformat() if debt.due_date else None,
            'description': debt.description
        }

        if debt.debt_type == 'i_owe':
            i_owe.append(debt_data)
        else:
            owed_to_me.append(debt_data)

    return jsonify({
        'i_owe': i_owe,
        'owed_to_me': owed_to_me
    })


# ==================== ANALYTICS ====================

@mobile_bp.route('/analytics', methods=['GET'])
@login_required
def get_analytics():
    """Get expense analytics by category"""
    days = request.args.get('days', 30, type=int)
    start_date = date.today() - timedelta(days=days)

    # Get transactions
    transactions = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id,
        BankingTransaction.date >= start_date,
        BankingTransaction.transaction_type == 'expense'
    ).all()

    # Group by category
    category_totals = {}
    for tx in transactions:
        category_id = tx.category_id or 0
        if category_id not in category_totals:
            category_totals[category_id] = Decimal(0)
        category_totals[category_id] += tx.amount

    # Format response
    analytics = []
    for category_id, total in category_totals.items():
        category = Category.query.get(category_id) if category_id > 0 else None

        analytics.append({
            'category_id': category_id,
            'category_name': category.name if category else 'Без категории',
            'total': float(total)
        })

    # Sort by total descending
    analytics.sort(key=lambda x: x['total'], reverse=True)

    return jsonify({
        'period_days': days,
        'total_expense': float(sum(category_totals.values())),
        'by_category': analytics
    })


# ==================== RECURRING PAYMENTS ====================

@mobile_bp.route('/recurring-payments', methods=['GET'])
@login_required
def get_recurring_payments():
    """Get upcoming recurring payments"""
    payments = RecurringPayment.query.filter_by(user_id=current_user.id).all()

    payments_data = []
    for payment in payments:
        payments_data.append({
            'id': payment.id,
            'description': payment.description,
            'amount': float(payment.amount),
            'currency': payment.currency,
            'frequency': payment.frequency,
            'next_due_date': payment.next_due_date.isoformat(),
            'counterparty': payment.counterparty
        })

    return jsonify({'payments': payments_data})


# ==================== NOTIFICATIONS ====================

@mobile_bp.route('/notifications', methods=['GET'])
@login_required
def get_notifications():
    """Get user notifications"""
    notifications = Notification.query.filter_by(
        user_id=current_user.id
    ).order_by(Notification.created_at.desc()).limit(20).all()

    notifications_data = []
    for notif in notifications:
        notifications_data.append({
            'id': notif.id,
            'type': notif.type,
            'title': notif.title,
            'message': notif.message,
            'link': notif.link,
            'is_read': notif.is_read,
            'created_at': notif.created_at.isoformat()
        })

    return jsonify({'notifications': notifications_data})


@mobile_bp.route('/notifications/<int:notif_id>/read', methods=['POST'])
@login_required
def mark_notification_read(notif_id):
    """Mark notification as read"""
    notification = Notification.query.filter_by(
        id=notif_id,
        user_id=current_user.id
    ).first()

    if not notification:
        return jsonify({'error': 'Notification not found'}), 404

    notification.is_read = True
    db.session.commit()

    return jsonify({'success': True})


# Error handlers
@mobile_bp.errorhandler(401)
def unauthorized(e):
    return jsonify({'error': 'Unauthorized'}), 401


@mobile_bp.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Not found'}), 404


@mobile_bp.errorhandler(500)
def internal_error(e):
    return jsonify({'error': 'Internal server error'}), 500
