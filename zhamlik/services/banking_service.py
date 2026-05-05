from datetime import datetime
from decimal import Decimal
from models import Account

def populate_account_from_form(account: Account, form_data: dict):
    """Вспомогательная функция для заполнения объекта Account из данных формы."""
    account.name = form_data.get('name')
    account.account_type = form_data.get('account_type')
    account.currency = form_data.get('currency')
    account.balance = Decimal(form_data.get('balance', '0'))
    account.is_active = 'is_active' in form_data
    account.is_external = 'is_external' in form_data
    interest_rate_str = form_data.get('interest_rate')
    account.bank_id = int(form_data.get('bank_id')) if form_data.get('bank_id') else None
    account.parent_id = int(form_data.get('parent_id')) if form_data.get('parent_id') else None
    account.interest_rate = Decimal(interest_rate_str) if interest_rate_str and interest_rate_str.strip() else None
    start_date_str = form_data.get('start_date')
    account.start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str and start_date_str.strip() else None
    end_date_str = form_data.get('end_date')
    account.end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str and end_date_str.strip() else None
    account.notes = form_data.get('notes')

    if account.account_type == 'credit':
        account.credit_limit = Decimal(form_data.get('credit_limit', '0'))
        account.grace_period_days = int(form_data.get('grace_period_days', '0'))
