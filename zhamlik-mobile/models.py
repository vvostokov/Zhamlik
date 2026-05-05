from extensions import db
from flask_login import UserMixin
from datetime import datetime

class User(UserMixin, db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)

class Account(db.Model):
    __tablename__ = 'account'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    account_type = db.Column(db.String(50), nullable=False)
    balance = db.Column(db.Numeric(20, 8), default=0)
    currency = db.Column(db.String(10), default='RUB')
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    is_external = db.Column(db.Boolean, default=False, nullable=False)
    bank_id = db.Column(db.Integer, db.ForeignKey('bank.id'))

class Bank(db.Model):
    __tablename__ = 'bank'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)

Account.bank = db.relationship('Bank', backref='accounts')

class BankingTransaction(db.Model):
    __tablename__ = 'banking_transaction'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'), nullable=False)
    amount = db.Column(db.Numeric(20, 8), nullable=False)
    transaction_type = db.Column(db.String(20), nullable=False)
    date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    description = db.Column(db.Text)
    category_id = db.Column(db.Integer, db.ForeignKey('category.id'))

    account = db.relationship('Account', backref='transactions')
    category = db.relationship('Category', backref='transactions')

class Category(db.Model):
    __tablename__ = 'category'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    type = db.Column(db.String(50), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

class Debt(db.Model):
    __tablename__ = 'debt'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    debt_type = db.Column(db.String(20), nullable=False)
    counterparty = db.Column(db.String(255), nullable=False)
    initial_amount = db.Column(db.Numeric(20, 8), nullable=False)
    repaid_amount = db.Column(db.Numeric(20, 8), default=0)
    currency = db.Column(db.String(16), nullable=False)
    due_date = db.Column(db.Date)
    status = db.Column(db.String(20), default='active')
    description = db.Column(db.Text)

class RecurringPayment(db.Model):
    __tablename__ = 'recurring_payment'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Numeric(20, 8), nullable=False)
    currency = db.Column(db.String(10), default='RUB')
    counterparty = db.Column(db.String(255))
    next_due_date = db.Column(db.Date)
