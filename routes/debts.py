from datetime import datetime, date, timezone, timedelta
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from flask import render_template, request, redirect, url_for, flash, current_app
from sqlalchemy.orm import joinedload
from sqlalchemy import func, asc, desc, or_

from routes import main_bp
from extensions import db
from models import Debt, RecurringPayment, Account, BankingTransaction, Category
from services.common import _get_or_create_category

from flask_login import login_required, current_user

def _create_debt_from_recurring_payment(payment: RecurringPayment):
    """Creates a new Debt record from a RecurringPayment."""
    current_app.logger.info(f"--- [Recurring Payments] Checking recurring payment: {payment.description} - {payment.next_due_date}")
    due_date = payment.next_due_date    
    current_app.logger.info(f"--- [Recurring Payments] Debt due date: {due_date}")
    existing_debt = Debt.query.filter_by(
        debt_type='i_owe',
        counterparty=payment.description,
        initial_amount=payment.amount,
        currency=payment.currency,
        due_date=due_date,
        user_id=payment.user_id
    ).first()

    if not existing_debt:
        new_debt = Debt(
            debt_type='i_owe',
            counterparty=payment.description,
            initial_amount=payment.amount,
            currency=payment.currency,
            due_date=due_date,
            user_id=payment.user_id,
            recurring_payment_id=payment.id
        )
        # Если у регулярного платежа есть контрагент, используем его
        if hasattr(payment, 'counterparty') and payment.counterparty:
             new_debt.counterparty = payment.counterparty
             new_debt.description = payment.description # Описание остается описанием

        db.session.add(new_debt)
        current_app.logger.info(f"--- [Recurring Payments] Создан новый долг для {payment.description} на сумму {payment.amount} {payment.currency} с датой погашения {due_date}.")

        # Обновляем дату следующего платежа
        interval = payment.interval_value
        if payment.frequency == 'daily':
            payment.next_due_date += timedelta(days=interval)
        elif payment.frequency == 'monthly':
            payment.next_due_date += relativedelta(months=interval)
        elif payment.frequency == 'yearly':
            payment.next_due_date += relativedelta(years=interval)
        
        db.session.add(payment)
    else:
        current_app.logger.info(f"--- [Recurring Payments] Долг для {payment.description} на сумму {payment.amount} {payment.currency} с датой погашения {due_date} уже существует.")

@main_bp.route('/debts')
@login_required
def ui_debts():
    i_owe_list = Debt.query.filter_by(debt_type='i_owe', user_id=current_user.id).order_by(Debt.status, Debt.due_date.asc()).all()
    owed_to_me_list = Debt.query.filter_by(debt_type='owed_to_me', user_id=current_user.id).order_by(Debt.status, Debt.due_date.asc()).all()

    i_owe_total = sum(d.initial_amount - d.repaid_amount for d in i_owe_list if d.status == 'active')
    owed_to_me_total = sum(d.initial_amount - d.repaid_amount for d in owed_to_me_list if d.status == 'active')

    # Fetch all recurring payments
    recurring_payments = RecurringPayment.query.filter_by(user_id=current_user.id).all()    

    # --- NEW LOGIC START: Data for Header and Counterparties Tab ---
    today = date.today()
    seven_days_from_now = today + timedelta(days=7)

    # 1. Upcoming Debts (I owe, active, due in next 7 days)
    upcoming_debts = [
        d for d in i_owe_list 
        if d.status == 'active' and d.due_date and today <= d.due_date <= seven_days_from_now
    ]
    upcoming_debts.sort(key=lambda d: d.due_date)

    # 2. Upcoming Recurring Payments (next due date in next 7 days)
    upcoming_recurring_payments = [
        p for p in recurring_payments 
        if today <= p.next_due_date <= seven_days_from_now
    ]
    upcoming_recurring_payments.sort(key=lambda p: p.next_due_date)

    # 3. Counterparty Balances
    # {counterparty: {currency: {'balance': Decimal, 'i_owe_exists': bool, 'owed_to_me_exists': bool}}}
    counterparty_data = defaultdict(lambda: defaultdict(lambda: {'balance': Decimal(0), 'i_owe_exists': False, 'owed_to_me_exists': False}))

    # Process Debts
    for debt in i_owe_list:
        if debt.status == 'active':
            remaining = debt.initial_amount - debt.repaid_amount
            # I owe: negative balance
            counterparty_data[debt.counterparty][debt.currency]['balance'] -= remaining
            counterparty_data[debt.counterparty][debt.currency]['i_owe_exists'] = True

    for debt in owed_to_me_list:
        if debt.status == 'active':
            remaining = debt.initial_amount - debt.repaid_amount
            # Owed to me: positive balance
            counterparty_data[debt.counterparty][debt.currency]['balance'] += remaining
            counterparty_data[debt.counterparty][debt.currency]['owed_to_me_exists'] = True
            
    # Format counterparty balances for template
    formatted_balances = []
    for counterparty, currency_balances in counterparty_data.items():
        for currency, data in currency_balances.items():
            # Display even if balance is zero so history can be accessed
            formatted_balances.append({
                'counterparty': counterparty,
                'currency': currency,
                'balance': data['balance'],
                'can_net': data['i_owe_exists'] and data['owed_to_me_exists']
            })
    
    # Sort by counterparty name
    formatted_balances.sort(key=lambda x: x['counterparty'])
    # --- NEW LOGIC END ---

    return render_template('debts.html', 
                           i_owe_list=i_owe_list, 
                           owed_to_me_list=owed_to_me_list,
                           i_owe_total=i_owe_total,
                           owed_to_me_total=owed_to_me_total,
                           recurring_payments=recurring_payments,
                           upcoming_debts=upcoming_debts,
                           upcoming_recurring_payments=upcoming_recurring_payments,
                           counterparty_balances=formatted_balances)

@main_bp.route('/debts/add', methods=['GET', 'POST'])
@login_required
def add_debt():
    """
    Создает долги из регулярных платежей, проверяя дату и создавая долг в запланированный день next_due_date.
    """
    current_app.logger.info("--- [MANUAL] add_debt called ---")
    # Manual trigger for current user's recurring payments could be added here, 
    # but the background task handles it for all users.
    # If we want to force check for current user:
    with current_app.app_context():
        current_app.logger.info("--- [add_debt] Running debt creation from recurring payments for current user ---")
        recurring_payments = RecurringPayment.query.filter_by(user_id=current_user.id).all()
        today = date.today()
        for payment in recurring_payments:
            days_until_due = (payment.next_due_date - today).days
            if 0 <= days_until_due <= 3:
                _create_debt_from_recurring_payment(payment)
    
        db.session.commit()        

    if request.method == 'POST':
        try:
            initial_amount = Decimal(request.form.get('initial_amount', '0'))
            if initial_amount <= 0:
                raise ValueError("Сумма долга должна быть положительной.")

            new_debt = Debt(
                debt_type=request.form['debt_type'],
                counterparty=request.form['counterparty'],
                initial_amount=initial_amount,
                currency=request.form['currency'],
                description=request.form.get('notes'),
                status='active',
                repaid_amount=Decimal(0),
                user_id=current_user.id
            )
            due_date_str = request.form.get('due_date')
            if due_date_str:
                new_debt.due_date = datetime.strptime(due_date_str, '%Y-%m-%d').date()
            
            db.session.add(new_debt)
            db.session.commit()
            flash('Долг успешно добавлен.', 'success')
            return redirect(url_for('main.ui_debts'))
        except (ValueError, InvalidOperation) as e:
            flash(f'Ошибка в данных: {e}', 'danger')
            return render_template('add_edit_debt.html', title="Добавить долг", debt=request.form)
    
    return render_template('add_edit_debt.html', title="Добавить долг", debt=None)

@main_bp.route('/debts/<int:debt_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_debt(debt_id):
    debt = Debt.query.filter_by(id=debt_id, user_id=current_user.id).first_or_404()
    if request.method == 'POST':
        try:
            initial_amount = Decimal(request.form.get('initial_amount', '0'))
            if initial_amount <= 0:
                raise ValueError("Сумма долга должна быть положительной.")
            
            debt.debt_type = request.form['debt_type']
            debt.counterparty = request.form['counterparty']
            debt.initial_amount = initial_amount
            debt.currency = request.form['currency']
            debt.description = request.form.get('notes')
            debt.status = request.form.get('status', 'active')
            
            due_date_str = request.form.get('due_date')
            debt.due_date = datetime.strptime(due_date_str, '%Y-%m-%d').date() if due_date_str else None
            
            db.session.commit()
            flash('Долг успешно обновлен.', 'success')
            return redirect(url_for('main.ui_debts'))
        except (ValueError, InvalidOperation) as e:
            flash(f'Ошибка в данных: {e}', 'danger')
            return render_template('add_edit_debt.html', title="Редактировать долг", debt=debt)

    return render_template('add_edit_debt.html', title="Редактировать долг", debt=debt)

@main_bp.route('/debts/<int:debt_id>/delete', methods=['POST'])
@login_required
def delete_debt(debt_id):
    debt = Debt.query.filter_by(id=debt_id, user_id=current_user.id).first_or_404()
    if debt.repayments.first():
        flash('Нельзя удалить долг, по которому есть операции погашения. Сначала удалите связанные банковские транзакции.', 'danger')
        return redirect(url_for('main.ui_debts'))
    
    db.session.delete(debt)
    db.session.commit()
    flash(f'Долг для "{debt.counterparty}" успешно удален.', 'success')
    return redirect(url_for('main.ui_debts'))

@main_bp.route('/debts/<int:debt_id>/repay', methods=['GET', 'POST'])
@login_required
def repay_debt(debt_id):
    debt = Debt.query.filter_by(id=debt_id, user_id=current_user.id).first_or_404()
    remaining_amount = debt.initial_amount - debt.repaid_amount
    
    # Находим встречные долги для взаимозачета
    opposite_type = 'owed_to_me' if debt.debt_type == 'i_owe' else 'i_owe'
    
    # Долги от того же контрагента
    same_counterparty_debts = Debt.query.filter(
        Debt.debt_type == opposite_type,
        Debt.currency == debt.currency,
        Debt.status == 'active',
        Debt.counterparty == debt.counterparty,
        Debt.user_id == current_user.id
    ).order_by(Debt.created_at.asc()).all()

    # Долги от других контрагентов (для опции "через другого контрагента")
    other_counterparty_debts = Debt.query.filter(
        Debt.debt_type == opposite_type,
        Debt.currency == debt.currency,
        Debt.status == 'active',
        Debt.counterparty != debt.counterparty,
        Debt.user_id == current_user.id
    ).order_by(Debt.counterparty, Debt.created_at.asc()).all()

    all_netting_debts = same_counterparty_debts + other_counterparty_debts

    if request.method == 'POST':
        # Проверяем, был ли выбран взаимозачет
        netting_debt_id = request.form.get('netting_debt_id')
        if netting_debt_id:
            # Логика взаимозачета
            try:
                netting_debt = Debt.query.filter_by(id=int(netting_debt_id), user_id=current_user.id).first()
                if not netting_debt:
                    flash('Встречный долг не найден.', 'danger')
                    return redirect(url_for('main.repay_debt', debt_id=debt.id))

                netting_remaining_amount = netting_debt.initial_amount - netting_debt.repaid_amount
                
                # Сумма взаимозачета - минимум из двух остатков
                netting_amount = min(remaining_amount, netting_remaining_amount)

                if netting_amount <= 0:
                    flash('Недостаточно остатка для взаимозачета.', 'danger')
                    return redirect(url_for('main.repay_debt', debt_id=debt.id))

                # 1. Обновляем текущий долг (который погашаем)
                debt.repaid_amount += netting_amount
                if debt.repaid_amount >= debt.initial_amount:
                    debt.status = 'repaid'
                    flash(f'Долг перед "{debt.counterparty}" полностью погашен взаимозачетом!', 'success')
                else:
                    flash(f'Частичное погашение долга перед "{debt.counterparty}" на сумму {netting_amount:.2f} {debt.currency} успешно зарегистрировано взаимозачетом.', 'success')

                # 2. Обновляем встречный долг
                netting_debt.repaid_amount += netting_amount
                if netting_debt.repaid_amount >= netting_debt.initial_amount:
                    netting_debt.status = 'repaid'
                    flash(f'Встречный долг с контрагентом "{netting_debt.counterparty}" также полностью погашен.', 'success')
                
                # 3. Commit changes
                db.session.commit()
                
                return redirect(url_for('main.ui_debts'))

            except Exception as e:
                db.session.rollback()
                current_app.logger.error(f"Error during debt netting: {e}")
                flash('Произошла ошибка при взаимозачете долга.', 'danger')
                return redirect(url_for('main.repay_debt', debt_id=debt.id))

        # --- СУЩЕСТВУЮЩАЯ ЛОГИКА ПОГАШЕНИЯ СЧЕТОМ ---
        try:
            amount = Decimal(request.form['amount'].replace(',', '.'))
            account_id = int(request.form['account_id'])
            date_str = request.form['date']
            description = request.form.get('description', f'Погашение долга: {debt.counterparty}')
            
            if amount <= 0:
                flash('Сумма погашения должна быть положительной.', 'danger')
                return redirect(url_for('main.repay_debt', debt_id=debt.id))

            if amount > remaining_amount:
                flash(f'Сумма погашения ({amount:.2f} {debt.currency}) превышает остаток долга ({remaining_amount:.2f} {debt.currency}).', 'danger')
                return redirect(url_for('main.repay_debt', debt_id=debt.id))

            account = Account.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
            
            # Determine transaction type and balance change
            if debt.debt_type == 'i_owe':
                # I owe -> I pay -> Expense, Account balance decreases
                tx_type = 'expense'
                account.balance -= amount
            else: # owed_to_me
                # Owed to me -> I receive -> Income, Account balance increases
                tx_type = 'income'
                account.balance += amount

            # Determine category
            if debt.recurring_payment_id and debt.recurring_payment_ref.category_ref:
                category = debt.recurring_payment_ref.category_ref
            else:
                if debt.debt_type == 'i_owe':
                    category = _get_or_create_category("Погашение долга", type='expense')
                else:
                    category = _get_or_create_category("Возврат долга", type='income')
            category_id = category.id

            # 2. Update Debt
            debt.repaid_amount += amount
            if debt.repaid_amount >= debt.initial_amount:
                debt.status = 'repaid'
                flash(f'Долг перед "{debt.counterparty}" полностью погашен!', 'success')
            else:
                flash(f'Долг перед "{debt.counterparty}" частично погашен.', 'success')

            # Создаем транзакцию погашения долга
            if description:
                tx_desc = f"Погашение долга: {description}"
            else:
                tx_desc = f"Погашение долга: {debt.description or ''}"
            
            new_tx = BankingTransaction(
                amount=amount,
                transaction_type=tx_type,
                date=datetime.strptime(date_str, '%Y-%m-%d').date(),
                description=tx_desc,
                account_id=account.id,
                debt_id=debt.id,
                category_id=category_id,
                counterparty=debt.counterparty,
                user_id=current_user.id
            )
            db.session.add(new_tx)
            
            db.session.commit()
            return redirect(url_for('main.ui_debts'))

        except InvalidOperation:
            flash('Неверный формат суммы.', 'danger')
            return redirect(url_for('main.repay_debt', debt_id=debt.id))

    accounts = Account.query.filter_by(currency=debt.currency, is_active=True, user_id=current_user.id).all()
    # Если нет счетов в валюте долга, показать все активные
    if not accounts:
        accounts = Account.query.filter_by(is_active=True, user_id=current_user.id).all()

    return render_template('repay_debt.html', debt=debt, accounts=accounts, now=datetime.now(), all_netting_debts=all_netting_debts)

@main_bp.route('/debts/net/<path:counterparty>/<string:currency>', methods=['GET', 'POST'])
@login_required
def ui_net_debt(counterparty, currency):
    """
    Страница для автоматического взаимозачета всех долгов с одним контрагентом в одной валюте.
    """
    # Декодируем counterparty
    from urllib.parse import unquote
    counterparty = unquote(counterparty)
    
    # Находим все активные долги
    debts_i_owe = Debt.query.filter_by(
        counterparty=counterparty, 
        currency=currency, 
        debt_type='i_owe', 
        status='active',
        user_id=current_user.id
    ).order_by(Debt.created_at).all()
    
    debts_owed_to_me = Debt.query.filter_by(
        counterparty=counterparty, 
        currency=currency, 
        debt_type='owed_to_me', 
        status='active',
        user_id=current_user.id
    ).order_by(Debt.created_at).all()
    
    if not debts_i_owe or not debts_owed_to_me:
        flash('Нет встречных долгов для взаимозачета.', 'warning')
        return redirect(url_for('main.ui_debts'))
        
    # Считаем общие суммы
    total_i_owe = sum(d.initial_amount - d.repaid_amount for d in debts_i_owe)
    total_owed_to_me = sum(d.initial_amount - d.repaid_amount for d in debts_owed_to_me)
    
    netting_amount = min(total_i_owe, total_owed_to_me)
    
    try:
        remaining_netting = netting_amount
        
        # Погашаем "я должен"
        for debt in debts_i_owe:
            if remaining_netting <= 0: break
            remaining_debt = debt.initial_amount - debt.repaid_amount
            amount_to_repay = min(remaining_debt, remaining_netting)
            
            debt.repaid_amount += amount_to_repay
            remaining_netting -= amount_to_repay
            
            if debt.initial_amount == debt.repaid_amount:
                debt.status = 'repaid'

        remaining_netting = netting_amount
        # Погашаем "мне должны"
        for debt in debts_owed_to_me:
            if remaining_netting <= 0: break
            remaining_debt = debt.initial_amount - debt.repaid_amount
            amount_to_repay = min(remaining_debt, remaining_netting)
            
            debt.repaid_amount += amount_to_repay
            remaining_netting -= amount_to_repay
            
            if debt.initial_amount == debt.repaid_amount:
                debt.status = 'repaid'

        db.session.commit()
        flash(f'Взаимозачет с контрагентом "{counterparty}" на сумму {netting_amount:,.2f} {currency} успешно выполнен.', 'success')
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Ошибка при взаимозачете долгов: {e}", exc_info=True)
        flash(f'Произошла ошибка при взаимозачете: {e}', 'danger')

    return redirect(url_for('main.ui_debts'))

@main_bp.route('/counterparty/<path:counterparty>/history')
@login_required
def ui_counterparty_history(counterparty):
    """Отображает историю транзакций и долгов по контрагенту."""
    # Декодируем counterparty из URL
    from urllib.parse import unquote
    counterparty = unquote(counterparty)
    if not counterparty:
        flash('Контрагент не указан.', 'danger')
        return redirect(url_for('main.ui_debts'))

    # Получить все долги по контрагенту
    debts = Debt.query.filter_by(counterparty=counterparty, user_id=current_user.id).order_by(Debt.created_at.desc()).all()

    # Получить все транзакции по контрагенту (counterparty или merchant)
    # Need to join Account to check user_id
    transactions = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(Account.user_id == current_user.id).options(
        joinedload(BankingTransaction.account_ref),
        joinedload(BankingTransaction.category_ref)
    ).filter(
        (BankingTransaction.counterparty == counterparty) | (BankingTransaction.merchant == counterparty)
    ).order_by(BankingTransaction.date.desc()).all()

    # Рассчитать общий баланс
    total_debt_balance = Decimal(0)
    for debt in debts:
        if debt.debt_type == 'i_owe':
            total_debt_balance -= (debt.initial_amount - debt.repaid_amount)
        else:
            total_debt_balance += (debt.initial_amount - debt.repaid_amount)

    # Валюты - предположим, что все в одной валюте, или показать по валютам
    currencies = set()
    for debt in debts:
        currencies.add(debt.currency)
    for tx in transactions:
        currencies.add(tx.account_ref.currency)

    return render_template('counterparty_history.html', counterparty=counterparty, debts=debts, transactions=transactions, total_debt_balance=total_debt_balance, currencies=currencies)

@main_bp.route('/recurring_payments/add', methods=['GET', 'POST'])
@login_required
def ui_add_recurring_payment():
    if request.method == 'POST':
        try:
            description = request.form['description']
            frequency = request.form['frequency']
            interval_value = int(request.form.get('interval_value', 1))
            amount = Decimal(request.form['amount'])
            currency = request.form['currency']
            next_due_date = datetime.strptime(request.form.get('next_due_date'), '%Y-%m-%d').date()
            counterparty = request.form.get('counterparty') or None
            category_id = request.form.get('category_id') or None

            payment = RecurringPayment(
                description=description,
                frequency=frequency,
                interval_value=interval_value,
                amount=amount,
                currency=currency,
                next_due_date=next_due_date,
                counterparty=counterparty,
                category_id=category_id,
                user_id=current_user.id
            )
            db.session.add(payment)
            db.session.commit()
            flash('Регулярный платеж успешно создан.', 'success')
            return redirect(url_for('main.ui_debts'))
        except (ValueError, InvalidOperation) as e:
            flash(f'Ошибка в данных: {e}', 'danger')
            
    # Fetch counterparties for autocomplete
    debt_counterparties = db.session.query(Debt.counterparty).filter(Debt.counterparty.isnot(None), Debt.user_id == current_user.id).distinct().all()
    counterparties = sorted([cp[0] for cp in debt_counterparties])

    categories = Category.query.filter_by(user_id=current_user.id, type='expense').order_by(Category.name).all()

    return render_template('recurring_payment_form.html', title="Создать регулярный платеж", payment={}, counterparties=counterparties, categories=categories)

@main_bp.route('/recurring_payments/<int:payment_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_recurring_payment(payment_id):
    payment = RecurringPayment.query.filter_by(id=payment_id, user_id=current_user.id).first_or_404()
    if request.method == 'POST':
        try:
            payment.description = request.form['description']
            payment.frequency = request.form['frequency']
            payment.interval_value = int(request.form.get('interval_value', 1))
            payment.amount = Decimal(request.form['amount'])
            payment.currency = request.form['currency']
            payment.next_due_date = datetime.strptime(request.form.get('next_due_date'), '%Y-%m-%d').date()
            payment.counterparty = request.form.get('counterparty') or None
            payment.category_id = request.form.get('category_id') or None

            db.session.commit()
            flash('Регулярный платеж обновлен.', 'success')
            return redirect(url_for('main.ui_debts'))
        except (ValueError, InvalidOperation) as e:
            flash(f'Ошибка в данных: {e}', 'danger')

    # Fetch counterparties for autocomplete
    debt_counterparties = db.session.query(Debt.counterparty).filter(Debt.counterparty.isnot(None), Debt.user_id == current_user.id).distinct().all()
    counterparties = sorted([cp[0] for cp in debt_counterparties])

    categories = Category.query.filter_by(user_id=current_user.id, type='expense').order_by(Category.name).all()

    return render_template('recurring_payment_form.html', title="Редактировать платеж", payment=payment, counterparties=counterparties, categories=categories)

@main_bp.route('/recurring_payments/<int:payment_id>/delete', methods=['POST'])
@login_required
def ui_delete_recurring_payment(payment_id):
    payment = RecurringPayment.query.filter_by(id=payment_id, user_id=current_user.id).first_or_404()
    db.session.delete(payment)
    db.session.commit()
    flash('Регулярный платеж удален.', 'success')
    return redirect(url_for('main.ui_debts'))
