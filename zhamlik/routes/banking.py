from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from flask import render_template, request, redirect, url_for, flash, current_app
from sqlalchemy.orm import joinedload
from sqlalchemy import asc, desc, or_

from routes import main_bp
from extensions import db
from models import Account, Bank, BankingTransaction, Category, Debt, TransactionItem
from services.banking_service import populate_account_from_form

from flask_login import login_required, current_user

@main_bp.route('/banking-overview')
@login_required
def ui_banking_overview():
    """Отображает объединенную страницу счетов и банков."""
    accounts = Account.query.filter_by(user_id=current_user.id).order_by(Account.is_active.desc(), Account.name).all()
    banks = Bank.query.order_by(Bank.name).all() # Banks are likely shared or public
    return render_template('banking_overview.html', accounts=accounts, banks=banks)

@main_bp.route('/accounts/add', methods=['GET', 'POST'])
@login_required
def add_account():
    """Обрабатывает добавление нового банковского счета (GET-форма, POST-создание)."""
    banks = Bank.query.order_by(Bank.name).all()
    all_accounts = Account.query.filter_by(user_id=current_user.id).order_by(Account.name).all()
    if request.method == 'POST':
        try:
            new_account = Account(user_id=current_user.id)
            populate_account_from_form(new_account, request.form)
            db.session.add(new_account)
            db.session.commit()
            flash(f'Счет "{new_account.name}" успешно создан.', 'success')
            return redirect(url_for('main.ui_banking_overview'))
        except (InvalidOperation, ValueError) as e:
            flash(f'Ошибка в данных: {e}', 'danger')
            current_data = request.form.to_dict()
            return render_template('add_edit_account.html', form_action_url=url_for('main.add_account'), account=None, title="Добавить новый счет", banks=banks, all_accounts=all_accounts, current_data=current_data)
    # GET request
    return render_template('add_edit_account.html', form_action_url=url_for('main.add_account'), account=None, title="Добавить новый счет", banks=banks, all_accounts=all_accounts)

@main_bp.route('/accounts/<int:account_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_account_form(account_id):
    account = Account.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    banks = Bank.query.order_by(Bank.name).all()
    all_accounts = Account.query.filter_by(user_id=current_user.id).order_by(Account.name).all()
    if request.method == 'POST':
        try:
            populate_account_from_form(account, request.form)
            db.session.commit()
            flash(f'Счет "{account.name}" успешно обновлен.', 'success')
            return redirect(url_for('main.ui_banking_overview'))
        except (InvalidOperation, ValueError) as e:
            flash(f'Ошибка в данных: {e}', 'danger')
            # Передаем измененные данные формы обратно в шаблон
            current_data = request.form.to_dict()
            return render_template('add_edit_account.html', form_action_url=url_for('main.ui_edit_account_form', account_id=account_id), account=account, title="Редактировать счет", banks=banks, all_accounts=all_accounts, current_data=current_data)
    return render_template('add_edit_account.html', form_action_url=url_for('main.ui_edit_account_form', account_id=account_id), account=account, title="Редактировать счет", banks=banks, all_accounts=all_accounts)

@main_bp.route('/accounts/<int:account_id>/delete', methods=['POST'])
@login_required
def ui_delete_account(account_id):
    account = Account.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    # Проверка, есть ли связанные транзакции
    if BankingTransaction.query.filter((BankingTransaction.account_id == account_id) | (BankingTransaction.to_account_id == account_id)).first():
        flash(f'Нельзя удалить счет "{account.name}", так как с ним связаны транзакции. Сначала удалите или перенесите транзакции.', 'danger')
        return redirect(url_for('main.ui_banking_overview'))
    
    db.session.delete(account)
    db.session.commit()
    flash(f'Счет "{account.name}" успешно удален.', 'success')
    return redirect(url_for('main.ui_banking_overview'))

@main_bp.route('/banks/add', methods=['GET', 'POST'])
@login_required
def ui_add_bank():
    # Banks are global for simplicity now
    """Обрабатывает добавление нового банка."""
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if not name:
            flash('Название банка не может быть пустым.', 'danger')
        elif Bank.query.filter_by(name=name).first():
            flash(f'Банк с названием "{name}" уже существует.', 'danger')
        else:
            db.session.add(Bank(name=name))
            db.session.commit()
            flash(f'Банк "{name}" успешно добавлен.', 'success')
            return redirect(url_for('main.ui_banking_overview'))
    return render_template('add_edit_bank.html', title="Добавить банк", bank=None)

@main_bp.route('/banks/<int:bank_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_bank(bank_id):
    """Обрабатывает редактирование банка."""
    bank = Bank.query.get_or_404(bank_id)
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if not name:
            flash('Название банка не может быть пустым.', 'danger')
        elif Bank.query.filter(Bank.id != bank_id, Bank.name == name).first():
            flash(f'Банк с названием "{name}" уже существует.', 'danger')
        else:
            bank.name = name
            db.session.commit()
            flash('Название банка успешно обновлено.', 'success')
            return redirect(url_for('main.ui_banking_overview'))
    return render_template('add_edit_bank.html', title="Редактировать банк", bank=bank)

@main_bp.route('/banks/<int:bank_id>/delete', methods=['POST'])
@login_required
def ui_delete_bank(bank_id):
    """Обрабатывает удаление банка."""
    bank = Bank.query.get_or_404(bank_id)
    if bank.accounts.first():
        flash(f'Нельзя удалить банк "{bank.name}", так как с ним связаны счета. Сначала измените или удалите связанные счета.', 'danger')
        return redirect(url_for('main.ui_banking_overview'))
    
    db.session.delete(bank)
    db.session.commit()
    flash(f'Банк "{bank.name}" успешно удален.', 'success')
    return redirect(url_for('main.ui_banking_overview'))

@main_bp.route('/banking-transactions')
@login_required
def ui_transactions():
    page = request.args.get('page', 1, type=int)
    sort_by = request.args.get('sort_by', 'date')
    order = request.args.get('order', 'desc')
    filter_account_id = request.args.get('filter_account_id', 'all')
    filter_type = request.args.get('filter_type', 'all')

    query = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(Account.user_id == current_user.id).options(
        joinedload(BankingTransaction.account_ref),
        joinedload(BankingTransaction.to_account_ref),
        joinedload(BankingTransaction.category_ref),
        # Eager load items and their categories to prevent N+1 queries in the template
        joinedload(BankingTransaction.items).joinedload(TransactionItem.category)
    )

    if filter_account_id != 'all':
        query = query.filter(BankingTransaction.account_id == int(filter_account_id))
    if filter_type != 'all':
        query = query.filter(BankingTransaction.transaction_type == filter_type)

    sort_column = getattr(BankingTransaction, sort_by, BankingTransaction.date)
    if order == 'desc':
        query = query.order_by(desc(sort_column))
    else:
        query = query.order_by(asc(sort_column))

    pagination = query.paginate(page=page, per_page=50, error_out=False)
    accounts = Account.query.filter_by(is_active=True, user_id=current_user.id).order_by(Account.name).all()
    unique_types = [r[0] for r in db.session.query(BankingTransaction.transaction_type).join(Account, BankingTransaction.account_id == Account.id).filter(Account.user_id == current_user.id).distinct().order_by(BankingTransaction.transaction_type).all()]

    return render_template('transactions.html', transactions=pagination.items, pagination=pagination, sort_by=sort_by, order=order, filter_account_id=filter_account_id, filter_type=filter_type, accounts=accounts, unique_types=unique_types)

@main_bp.route('/transactions/add', methods=['GET', 'POST'])
@login_required
def ui_add_transaction_form():
    if request.method == 'POST':
        tx_type = request.form.get('transaction_type')
        try:
            account_id = int(request.form.get('account_id'))
            account = Account.query.filter_by(id=account_id, user_id=current_user.id).first()
            if not account:
                raise ValueError("Счет не найден.")

            if tx_type == 'expense':
                amount = Decimal(request.form.get('amount', '0'))
                if amount <= 0: raise ValueError("Сумма должна быть положительной.")
                
                if account.account_type == 'credit':
                    account.balance += amount
                else:
                    account.balance -= amount

                new_tx = BankingTransaction(
                    transaction_type=tx_type,
                    amount=amount,
                    date=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                    description=request.form.get('description'),
                    account_id=int(request.form.get('account_id')),
                    category_id=int(request.form.get('category_id')) if request.form.get('category_id') else None,
                    counterparty=request.form.get('counterparty') or None,
                    user_id=current_user.id
                )
                db.session.add(new_tx)

                # --- АВТОМАТИЧЕСКОЕ СОЗДАНИЕ ДОЛГА ---
                category_id = int(request.form.get('category_id')) if request.form.get('category_id') else None
                if category_id:
                    category = Category.query.get(category_id)
                    if category and 'долг' in category.name.lower():
                        debt_type = 'owed_to_me' if tx_type == 'expense' else 'i_owe'
                        counterparty_name = request.form.get('counterparty')
                        
                        if counterparty_name:
                            new_debt = Debt(
                                debt_type=debt_type,
                                counterparty=counterparty_name,
                                initial_amount=amount,
                                currency=account.currency,
                                status='active',
                                description=f"Автоматически создан из транзакции: {request.form.get('description') or ''}",
                                created_at=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                                user_id=current_user.id
                            )
                            db.session.add(new_debt)
                            flash(f'Автоматически создан долг для контрагента "{counterparty_name}".', 'info')
                        else:
                             flash('Для создания долга необходимо указать контрагента.', 'warning')
                # -------------------------------------
            
            elif tx_type == 'income':
                amount = Decimal(request.form.get('amount', '0'))
                if amount <= 0: raise ValueError("Сумма должна быть положительной.")

                if account.account_type == 'credit':
                    account.balance -= amount
                else:
                    account.balance += amount

                new_tx = BankingTransaction(
                    transaction_type=tx_type,
                    amount=amount,
                    date=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                    description=request.form.get('description'),
                    account_id=int(request.form.get('account_id')),
                    category_id=int(request.form.get('category_id')) if request.form.get('category_id') else None,
                    counterparty=request.form.get('counterparty') or None,
                    user_id=current_user.id
                )
                db.session.add(new_tx)

                # --- АВТОМАТИЧЕСКОЕ СОЗДАНИЕ ДОЛГА (INCOME) ---
                category_id = int(request.form.get('category_id')) if request.form.get('category_id') else None
                if category_id:
                    category = Category.query.get(category_id)
                    if category and 'долг' in category.name.lower():
                        debt_type = 'i_owe'
                        counterparty_name = request.form.get('counterparty')
                        
                        if counterparty_name:
                            new_debt = Debt(
                                debt_type=debt_type,
                                counterparty=counterparty_name,
                                initial_amount=amount,
                                currency=account.currency,
                                status='active',
                                description=f"Автоматически создан из транзакции: {request.form.get('description') or ''}",
                                created_at=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                                user_id=current_user.id
                            )
                            db.session.add(new_debt)
                            flash(f'Автоматически создан долг для контрагента "{counterparty_name}".', 'info')
                        else:
                             flash('Для создания долга необходимо указать контрагента.', 'warning')
                # ----------------------------------------------

            elif tx_type == 'transfer':
                amount = Decimal(request.form.get('amount', '0'))
                if amount <= 0: raise ValueError("Сумма должна быть положительной.")
                
                from_account_id = int(request.form.get('account_id'))
                to_account_id = int(request.form.get('to_account_id'))
                if from_account_id == to_account_id: raise ValueError("Счета для перевода должны отличаться.")

                from_account = account # Already fetched
                to_account = Account.query.filter_by(id=to_account_id, user_id=current_user.id).first()
                if not to_account:
                    raise ValueError("Счет зачисления не найден.")

                if from_account.account_type == 'credit':
                    from_account.balance += amount
                else:
                    from_account.balance -= amount
                
                if to_account.account_type == 'credit':
                    to_account.balance -= amount
                else:
                    to_account.balance += amount

                new_tx = BankingTransaction(
                    transaction_type=tx_type,
                    amount=amount,
                    date=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                    description=request.form.get('description'),
                    account_id=from_account_id,
                    to_account_id=to_account_id,
                    counterparty=request.form.get('counterparty') or None,
                    user_id=current_user.id
                )
                db.session.add(new_tx)

            elif tx_type == 'exchange':
                from_amount = Decimal(request.form.get('amount', '0'))
                to_amount = Decimal(request.form.get('to_amount', '0'))
                if from_amount <= 0 or to_amount <= 0:
                    raise ValueError("Суммы для обмена должны быть положительными.")
                
                from_account_id = int(request.form.get('account_id'))
                to_account_id = int(request.form.get('to_account_id'))
                if from_account_id == to_account_id:
                    raise ValueError("Счета для обмена должны отличаться.")

                from_account = account # Already fetched
                to_account = Account.query.filter_by(id=to_account_id, user_id=current_user.id).first()
                if not to_account: raise ValueError("Счет зачисления не найден.")
                from_account.balance -= from_amount
                to_account.balance += to_amount

                new_tx = BankingTransaction(
                    transaction_type=tx_type,
                    amount=from_amount,
                    to_amount=to_amount,
                    date=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                    description=request.form.get('description'),
                    account_id=from_account_id,
                    to_account_id=to_account_id,
                    counterparty=request.form.get('counterparty') or None,
                    user_id=current_user.id
                )
                db.session.add(new_tx)
            elif tx_type in ['purchase', 'manual_purchase']:
                item_names = request.form.getlist('item_name[]')
                item_quantities = request.form.getlist('item_quantity[]')
                item_prices = request.form.getlist('item_price[]')
                item_categories = request.form.getlist('item_category_id[]')

                if not item_names: raise ValueError("В покупке должен быть хотя бы один товар.")

                total_purchase_amount = sum(
                    Decimal(qty) * Decimal(price) for qty, price in zip(item_quantities, item_prices)
                )

                account.balance -= total_purchase_amount

                purchase_tx = BankingTransaction(
                    transaction_type='expense',
                    amount=total_purchase_amount,
                    date=datetime.strptime(request.form.get('date'), '%Y-%m-%dT%H:%M'),
                    description=request.form.get('description'),
                    merchant=request.form.get('merchant'),
                    account_id=int(request.form.get('account_id')),
                    counterparty=request.form.get('counterparty') or None,
                    user_id=current_user.id
                )
                db.session.add(purchase_tx)
                db.session.flush()

                for i in range(len(item_names)):
                    quantity = Decimal(item_quantities[i])
                    price = Decimal(item_prices[i])
                    category_id = int(item_categories[i]) if item_categories[i] else None
                    
                    item = TransactionItem(
                        name=item_names[i],
                        quantity=quantity,
                        price=price,
                        total=quantity * price,
                        transaction_id=purchase_tx.id,
                        category_id=category_id
                    )
                    db.session.add(item)
            
            else:
                raise ValueError("Неизвестный тип транзакции.")

            db.session.commit()
            flash('Транзакция успешно добавлена.', 'success')
            return redirect(url_for('main.ui_transactions'))

        except (ValueError, InvalidOperation) as e:
            db.session.rollback()
            flash(f'Ошибка в данных: {e}', 'danger')
    
    accounts = Account.query.filter_by(is_active=True, user_id=current_user.id).order_by(Account.name).all()
    expense_categories = Category.query.filter_by(type='expense', parent_id=None).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).options(joinedload(Category.subcategories)).all()
    income_categories = Category.query.filter_by(type='income', parent_id=None).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).options(joinedload(Category.subcategories)).all()
    categories = Category.query.filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).all()

    # Собираем список контрагентов из долгов и транзакций
    debt_counterparties = db.session.query(Debt.counterparty).filter(Debt.counterparty.isnot(None), Debt.user_id == current_user.id).distinct().all()
    tx_counterparties = db.session.query(BankingTransaction.counterparty).join(Account, BankingTransaction.account_id == Account.id).filter(BankingTransaction.counterparty.isnot(None), Account.user_id == current_user.id).distinct().all()
    tx_merchants = db.session.query(BankingTransaction.merchant).join(Account, BankingTransaction.account_id == Account.id).filter(BankingTransaction.merchant.isnot(None), Account.user_id == current_user.id).distinct().all()
    counterparties = set()
    for cp in debt_counterparties + tx_counterparties + tx_merchants:
        counterparties.add(cp[0])
    counterparties = sorted(list(counterparties))

    return render_template(
        'add_transaction.html',
        accounts=accounts,
        expense_categories=expense_categories,
        income_categories=income_categories,
        counterparties=counterparties,
        now=datetime.now(timezone.utc)
    )    

@main_bp.route('/transactions/<int:tx_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_transaction_form(tx_id):
    transaction = BankingTransaction.query.join(Account, BankingTransaction.account_id == Account.id).filter(Account.user_id == current_user.id, BankingTransaction.id == tx_id).options(joinedload(BankingTransaction.items)).first_or_404()
    
    if request.method == 'POST':
        try:
            # Обновление общих полей
            transaction.date = datetime.strptime(request.form['date'], '%Y-%m-%dT%H:%M')
            transaction.description = request.form.get('description')
            transaction.counterparty = request.form.get('counterparty') or None

            # Логика обновления баланса только для простых типов (Расход/Доход)
            # Для переводов и обменов пока поддерживается только изменение описания/даты/контрагента
            if transaction.transaction_type in ['expense', 'income']:
                old_amount = transaction.amount
                old_account_id = transaction.account_id
                
                # Получаем новые значения
                new_account_id = int(request.form['account_id'])
                
                # Обработка суммы (через товары или напрямую)
                new_amount = Decimal(0)
                if transaction.items:
                     # Обновление товаров
                     item_names = request.form.getlist('item_name[]')
                     item_quantities = request.form.getlist('item_quantity[]')
                     item_prices = request.form.getlist('item_price[]')
                     item_categories = request.form.getlist('item_category_id[]')
                     
                     # Удаляем старые (проще всего)
                     TransactionItem.query.filter_by(transaction_id=transaction.id).delete()
                     
                     for i in range(len(item_names)):
                         qty = Decimal(item_quantities[i])
                         price = Decimal(item_prices[i])
                         total = qty * price
                         new_amount += total
                         
                         cat_id = int(item_categories[i]) if item_categories[i] else None
                         new_item = TransactionItem(name=item_names[i], quantity=qty, price=price, total=total, transaction_id=transaction.id, category_id=cat_id)
                         db.session.add(new_item)
                else:
                     new_amount = Decimal(request.form['amount'])
                     transaction.category_id = int(request.form['category_id']) if request.form.get('category_id') else None

                # Если сумма или счет изменились - корректируем балансы
                if old_amount != new_amount or old_account_id != new_account_id:
                    # 1. Откат старой транзакции
                    old_account = Account.query.get(old_account_id)
                    if transaction.transaction_type == 'expense':
                        if old_account.account_type == 'credit': old_account.balance += old_amount
                        else: old_account.balance += old_amount
                    elif transaction.transaction_type == 'income':
                        if old_account.account_type == 'credit': old_account.balance -= old_amount
                        else: old_account.balance -= old_amount
                    
                    # 2. Применение новой транзакции
                    new_account = Account.query.get(new_account_id)
                    if transaction.transaction_type == 'expense':
                        if new_account.account_type == 'credit': new_account.balance -= new_amount
                        else: new_account.balance -= new_amount
                    elif transaction.transaction_type == 'income':
                        if new_account.account_type == 'credit': new_account.balance += new_amount
                        else: new_account.balance += new_amount
                
                # Обновляем сумму и счет в объекте транзакции
                transaction.amount = new_amount
                transaction.account_id = new_account_id
                if transaction.transaction_type == 'expense':
                    # Если это расход, обновляем также категорию, если она была изменена
                    transaction.category_id = int(request.form['category_id']) if request.form.get('category_id') else None

            db.session.commit()
            flash('Транзакция успешно обновлена.', 'success')
            return redirect(url_for('main.ui_transactions'))

        except (ValueError, InvalidOperation) as e:
            db.session.rollback()
            flash(f'Ошибка при обновлении: {e}', 'danger')

    accounts = Account.query.filter_by(user_id=current_user.id).order_by(Account.name).all()
    categories = Category.query.filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).all()
    expense_categories = Category.query.filter_by(type='expense', parent_id=None).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).options(joinedload(Category.subcategories)).all()

    # Fetch counterparties
    debt_counterparties = db.session.query(Debt.counterparty).filter(Debt.counterparty.isnot(None), Debt.user_id == current_user.id).distinct().all()
    tx_counterparties = db.session.query(BankingTransaction.counterparty).join(Account, BankingTransaction.account_id == Account.id).filter(BankingTransaction.counterparty.isnot(None), Account.user_id == current_user.id).distinct().all()
    tx_merchants = db.session.query(BankingTransaction.merchant).join(Account, BankingTransaction.account_id == Account.id).filter(BankingTransaction.merchant.isnot(None), Account.user_id == current_user.id).distinct().all()
    counterparties = set()
    for cp in debt_counterparties + tx_counterparties + tx_merchants:
        counterparties.add(cp[0])
    counterparties = sorted(list(counterparties))

    return render_template('edit_transaction.html', transaction=transaction, accounts=accounts, categories=categories, expense_categories=expense_categories, counterparties=counterparties)

@main_bp.route('/cashback_rules')
@login_required
def ui_cashback_rules():
    # Placeholder for cashback rules page
    return render_template('cashback_rules.html', rules=[])

@main_bp.route('/cashback_rules/add', methods=['GET', 'POST'])
@login_required
def ui_add_cashback_rule_form():
    # Placeholder
    return render_template('add_cashback_rule.html', rule={})

@main_bp.route('/cashback_rules/<int:rule_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_cashback_rule_form(rule_id):
    # Placeholder
    return render_template('edit_cashback_rule.html', rule={})

@main_bp.route('/categories')
@login_required
def ui_categories():
    expense_parents = Category.query.filter_by(type='expense', parent_id=None).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).options(joinedload(Category.subcategories)).all()
    income_parents = Category.query.filter_by(type='income', parent_id=None).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.name).all()
    return render_template('categories.html', expense_parents=expense_parents, income_parents=income_parents)

@main_bp.route('/categories/add', methods=['GET', 'POST'])
@login_required
def ui_add_category_form():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        cat_type = request.form.get('type', 'expense').strip()
        parent_id = request.form.get('parent_id')
        if not name:
            flash('Название категории не может быть пустым.', 'danger')
        else:
            existing = Category.query.filter_by(name=name, type=cat_type, user_id=current_user.id).first()
            if existing:
                flash(f'Категория "{name}" с типом "{cat_type}" уже существует.', 'danger')
            else:
                new_category = Category(name=name, type=cat_type, parent_id=int(parent_id) if parent_id else None, user_id=current_user.id)
                db.session.add(new_category)
                db.session.commit()
                flash(f'Категория "{name}" успешно добавлена.', 'success')
                return redirect(url_for('main.ui_categories'))
    
    parent_categories = Category.query.filter_by(parent_id=None).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.type, Category.name).all()
    return render_template('add_edit_category.html', title="Добавить категорию", category=None, parent_categories=parent_categories)

@main_bp.route('/categories/<int:category_id>/edit', methods=['GET', 'POST'])
@login_required
def ui_edit_category_form(category_id):
    category = Category.query.filter_by(id=category_id, user_id=current_user.id).first_or_404()
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        cat_type = request.form.get('type', 'expense').strip()
        parent_id = request.form.get('parent_id')
        if not name:
            flash('Название категории не может быть пустым.', 'danger')
        else:

            existing = Category.query.filter(
                Category.id != category_id,
                Category.name == name,
                Category.type == cat_type,
                Category.user_id == current_user.id
            ).first()
            if existing:
                flash(f'Категория "{name}" с типом "{cat_type}" уже существует.', 'danger')
            else:
                category.name = name
                category.type = cat_type
                category.parent_id = int(parent_id) if parent_id else None
                db.session.commit()
                flash(f'Категория "{name}" успешно обновлена.', 'success')
                return redirect(url_for('main.ui_categories'))
    parent_categories = Category.query.filter(Category.parent_id.is_(None), Category.id != category_id).filter((Category.user_id == current_user.id) | (Category.user_id == None)).order_by(Category.type, Category.name).all()
    return render_template('add_edit_category.html', title="Редактировать категорию", category=category, parent_categories=parent_categories)

@main_bp.route('/categories/<int:category_id>/delete', methods=['POST'])
@login_required
def ui_delete_category(category_id):
    category = Category.query.filter_by(id=category_id, user_id=current_user.id).first_or_404()
    if BankingTransaction.query.filter_by(category_id=category_id).first() or \
       TransactionItem.query.filter_by(category_id=category_id).first():
        flash(f'Нельзя удалить категорию "{category.name}", так как она используется в транзакциях.', 'danger')
        return redirect(url_for('main.ui_categories'))
    
    db.session.delete(category)
    db.session.commit()
    flash(f'Категория "{category.name}" успешно удалена.', 'success')
    return redirect(url_for('main.ui_categories'))
