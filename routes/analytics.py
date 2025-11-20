from datetime import datetime, date, timedelta
from decimal import Decimal
from flask import render_template, request, redirect, url_for, flash, current_app
from sqlalchemy import func, desc
from sqlalchemy.orm import joinedload
from collections import defaultdict

from routes import main_bp
from extensions import db
from models import Account, BankingTransaction, Category, TransactionItem
from services.common import _get_currency_rates

from flask_login import login_required, current_user

@main_bp.route('/analytics')
@login_required
def ui_analytics_overview():    
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    # Default to last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            pass # Keep default
            
    if end_date_str:
        try:
            # Add 23:59:59 to end date to include the whole day
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        except ValueError:
            pass # Keep default

    # --- 1. Рассчитать общий баланс по всем активным банковским счетам ---
    currency_rates_to_rub = _get_currency_rates()

    # --- 1. Рассчитать общий баланс по всем активным ЛИЧНЫМ банковским счетам ---
    # Фильтруем только счета, которые НЕ являются внешними
    personal_bank_accounts = Account.query.filter(
        Account.account_type.in_(['bank_account', 'deposit', 'bank_card', 'credit']),
        Account.is_external == False,
        Account.user_id == current_user.id
    ).all()
    
    # Получаем внешние счета для отдельной вкладки
    external_accounts = Account.query.filter(
        Account.account_type.in_(['bank_account', 'deposit', 'bank_card', 'credit']),
        Account.is_external == True,
        Account.user_id == current_user.id
    ).all()

    total_balance_rub = Decimal(0)
    for acc in  personal_bank_accounts:
        value_in_rub = acc.balance * currency_rates_to_rub.get(acc.currency, Decimal(1.0))
        if acc.account_type == 'credit':
            total_balance_rub -= value_in_rub  # Вычесть долг по кредитной карте
        else:
            total_balance_rub += value_in_rub  # Добавить активы

    # Use dynamic start_date instead of hardcoded one_month_ago
    # one_month_ago = datetime.now() - timedelta(days=30) # Removed
    
    # --- 2. Получить банковские транзакции за выбранный период ---
    # three_months_ago = datetime.now() - timedelta(days=90) # Removed
    recent_transactions = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id, 
        BankingTransaction.date >= start_date,
        BankingTransaction.date <= end_date
    ).order_by(BankingTransaction.date.desc()).limit(100).all()

    # --- 3. Рассчитать расходы по категориям за выбранный период ---
    category_spending = db.session.query(
        Category.name,
        func.sum(BankingTransaction.amount)
    ).join(Category, BankingTransaction.category_id == Category.id).join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id,
        BankingTransaction.date >= start_date,
        BankingTransaction.date <= end_date,
        BankingTransaction.transaction_type == 'expense'
    ).group_by(Category.name).order_by(func.sum(BankingTransaction.amount).desc()).limit(10).all()

    category_labels = [item[0] for item in category_spending]
    category_data = [float(item[1]) for item in category_spending]

    # Calculate percentages
    total_spending = sum(category_data)
    
    if total_spending and category_data:
      category_percentages = [round((float(data) / float(total_spending)) * 100, 2) for data in category_data]
    else:
      category_percentages = [0.0] * len(category_data)
    
    purchase_category_spending = db.session.query(
        Category.name,
        func.sum(TransactionItem.total)
    ).join(Category, TransactionItem.category_id == Category.id).join(BankingTransaction, TransactionItem.transaction_id == BankingTransaction.id).join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id,
        BankingTransaction.date >= start_date,
        BankingTransaction.date <= end_date,
        BankingTransaction.transaction_type == 'expense'
    ).group_by(Category.name).order_by(func.sum(TransactionItem.total).desc()).limit(10).all()

    purchase_total_spending = sum(item[1] for item in purchase_category_spending)
    purchase_category_labels = [item[0] for item in purchase_category_spending]
    purchase_category_data = [float(item[1]) for item in purchase_category_spending]

    if purchase_total_spending and purchase_category_data:
        purchase_category_percentages = [round((float(data) / float(purchase_total_spending)) * 100, 2) for data in purchase_category_data]
    else:
        purchase_category_percentages = [0.0] * len(purchase_category_data)


    # --- 4. Объединение общих расходов по категориям (BankingTransaction + TransactionItem) ---
    combined_spending_map = defaultdict(Decimal)

    # 1. Добавить расходы из BankingTransaction (без детализации)
    for category_name, amount in category_spending:
        combined_spending_map[category_name] += amount

    # 2. Добавить расходы из TransactionItem (детализация)
    for category_name, amount in purchase_category_spending:
        combined_spending_map[category_name] += amount

    # Сортировка и подготовка данных для графика
    combined_spending_list = sorted(combined_spending_map.items(), key=lambda item: item[1], reverse=True)
    
    # Ограничение до 10 лучших категорий
    combined_spending_list = combined_spending_list[:10]

    combined_category_labels = [item[0] for item in combined_spending_list]
    combined_category_data = [float(item[1]) for item in combined_spending_list]
    
    combined_total_spending = sum(combined_category_data)
    if combined_total_spending:
        combined_category_percentages = [round((data / combined_total_spending) * 100, 2) for data in combined_category_data]
    else:
        combined_category_percentages = [0.0] * len(combined_category_data)


    # Получить детализированные данные о расходах по подкатегориям
    subcategory_spending = db.session.query(
        Category.name,
            func.sum(BankingTransaction.amount)
        ).join(Category, BankingTransaction.category_id == Category.id).join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id,
        BankingTransaction.date >= start_date,
        BankingTransaction.date <= end_date,
        BankingTransaction.transaction_type == 'expense',
        Category.parent_id.isnot(None)
    ).group_by(Category.name).order_by(func.sum(BankingTransaction.amount).desc()).limit(10).all()

    subcategory_labels = [item[0] for item in subcategory_spending]
    subcategory_data = [float(item[1]) for item in subcategory_spending]

    # Временная имитация данных о продуктах
    products_data = [10, 20, 15, 25, 30]
    products_labels = ["Product A", "Product B", "Product C", "Product D", "Product E"]

    # --- 4. Расчет общего денежного потока (Income vs Expense) за выбранный период ---
    cash_flow_data = db.session.query(
        BankingTransaction.transaction_type,
        func.sum(BankingTransaction.amount)
    ).join(Account, BankingTransaction.account_id == Account.id).filter(
        Account.user_id == current_user.id,
        BankingTransaction.date >= start_date,
        BankingTransaction.date <= end_date, 
        BankingTransaction.transaction_type.in_(['income', 'expense'])
    ).group_by(BankingTransaction.transaction_type).all()

    income_total = next((item[1] for item in cash_flow_data if item[0] == 'income'), Decimal(0))
    expense_total = next((item[1] for item in cash_flow_data if item[0] == 'expense'), Decimal(0))
    cash_flow_values = [float(income_total), float(expense_total)]
    
    # Add net_cash_flow calculation
    net_cash_flow = income_total - expense_total

    return render_template(
        'analytics_overview.html',
        start_date=start_date.strftime('%Y-%m-%d'), # Pass strings for date inputs
        end_date=end_date.strftime('%Y-%m-%d'),
        total_balance_rub=total_balance_rub,
        recent_transactions=recent_transactions,
        category_labels=category_labels,
        category_data=category_data,
        category_percentages=category_percentages,
        cash_flow_values=cash_flow_values,
        net_cash_flow=net_cash_flow, # Pass this variable to template
        total_income=income_total,   # Also pass total income
        total_expense=expense_total, # And total expense
        purchase_category_labels=purchase_category_labels,
        purchase_category_data=purchase_category_data,
        purchase_category_percentages=purchase_category_percentages,
        combined_category_labels=combined_category_labels,
        combined_category_data=combined_category_data,
        combined_category_percentages=combined_category_percentages,
        subcategory_labels=subcategory_labels,
        subcategory_data=subcategory_data,
        products_labels=products_labels,
        products_data=products_data
    )

@main_bp.route('/analytics/refresh-securities-history', methods=['POST'])
@login_required
def ui_refresh_securities_history():
    """Запускает пересчет истории стоимости портфеля ценных бумаг."""
    from analytics_logic import refresh_securities_portfolio_history
    success, message = refresh_securities_portfolio_history()
    if success:
        flash(message, 'success')
    else:
        flash(message, 'danger')
    return redirect(url_for('main.index'))
