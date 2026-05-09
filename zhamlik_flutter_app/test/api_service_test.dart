import 'package:flutter_test/flutter_test.dart';
import 'package:zhamlik/services/api_service.dart';
import 'package:zhamlik/models/transaction.dart';
import 'package:zhamlik/models/account.dart';
import 'package:zhamlik/models/debt.dart';
import 'package:zhamlik/models/overview.dart';

/// Unit Tests for API Service
///
/// Tests API methods in isolation
void main() {
  group('Transaction Model Tests', () {
    test('Transaction.fromJson should handle missing date field', () {
      // API sometimes returns transactions without date field
      final json = {
        'id': 1,
        'amount': 100.0,
        'type': 'expense',
        'description': 'Test',
        // 'date' is missing
      };

      final transaction = Transaction.fromJson(json);

      expect(transaction.id, 1);
      expect(transaction.amount, 100.0);
      expect(transaction.type, 'expense');
      expect(transaction.description, 'Test');
      expect(transaction.date, isNotNull); // Should default to DateTime.now()
    });

    test('Transaction.fromJson should parse date when present', () {
      final json = {
        'id': 1,
        'amount': 100.0,
        'type': 'income',
        'date': '2026-02-21T10:00:00',
      };

      final transaction = Transaction.fromJson(json);

      expect(transaction.id, 1);
      expect(transaction.date, isNotNull);
      expect(transaction.type, 'income');
    });

    test('Transaction type should be correctly identified', () {
      final incomeJson = {'id': 1, 'amount': 100.0, 'type': 'income'};
      final expenseJson = {'id': 2, 'amount': 50.0, 'type': 'expense'};
      final transferJson = {'id': 3, 'amount': 200.0, 'type': 'transfer'};

      final income = Transaction.fromJson(incomeJson);
      final expense = Transaction.fromJson(expenseJson);
      final transfer = Transaction.fromJson(transferJson);

      expect(income.type, 'income');
      expect(expense.type, 'expense');
      expect(transfer.type, 'transfer');
    });
  });

  group('Account Model Tests', () {
    test('Account.isActive should default to true', () {
      final json = {
        'id': 1,
        'name': 'Test Account',
        'type': 'bank_card',
        'currency': 'RUB',
        'balance': 1000.0,
        // 'is_active' is missing
      };

      final account = Account.fromJson(json);

      expect(account.isActive, true); // Should default to true
    });

    test('Account should parse is_active when present', () {
      final json = {
        'id': 1,
        'name': 'Test Account',
        'type': 'bank_card',
        'currency': 'RUB',
        'balance': 1000.0,
        'is_active': false,
      };

      final account = Account.fromJson(json);

      expect(account.isActive, false);
    });
  });

  group('Debt Model Tests', () {
    test('Debt should parse all fields correctly', () {
      final json = {
        'id': 1,
        'counterparty': 'John Doe',
        'amount': 5000.0,
        'is_i_owe': true,
        'description': 'Loan',
        'due_date': '2026-03-01',
        'is_paid': false,
      };

      final debt = Debt.fromJson(json);

      expect(debt.id, 1);
      expect(debt.counterparty, 'John Doe');
      expect(debt.amount, 5000.0);
      expect(debt.isIOwe, true);
      expect(debt.description, 'Loan');
      expect(debt.dueDate, isNotNull);
      expect(debt.isPaid, false);
    });

    test('Debt should handle optional fields', () {
      final json = {
        'id': 1,
        'counterparty': 'Jane Doe',
        'amount': 3000.0,
        'is_i_owe': false,
        // description, due_date, is_paid missing
      };

      final debt = Debt.fromJson(json);

      expect(debt.id, 1);
      expect(debt.counterparty, 'Jane Doe');
      expect(debt.amount, 3000.0);
      expect(debt.isIOwe, false);
      expect(debt.description, isNull);
      expect(debt.dueDate, isNull);
    });
  });

  group('Overview Model Tests', () {
    test('Overview should handle all optional fields', () {
      final json = {
        'total_balance': 10000.0,
        'monthly_income': 5000.0,
        'monthly_expense': 2000.0,
        'accounts': [
          {
            'id': 1,
            'name': 'Account 1',
            'type': 'bank_card',
            'currency': 'RUB',
            'balance': 5000.0,
          }
        ],
        'recent_transactions': [
          {
            'id': 1,
            'amount': 100.0,
            'type': 'expense',
          }
        ],
      };

      final overview = Overview.fromJson(json);

      expect(overview.totalBalance, 10000.0);
      expect(overview.monthlyIncome, 5000.0);
      expect(overview.monthlyExpense, 2000.0);
      expect(overview.accounts.length, 1);
      expect(overview.recentTransactions.length, 1);
    });

    test('Overview should handle missing accounts', () {
      final json = {
        'total_balance': 10000.0,
        // accounts missing
        'recent_transactions': [],
      };

      final overview = Overview.fromJson(json);

      expect(overview.totalBalance, 10000.0);
      expect(overview.accounts, isEmpty);
      expect(overview.recentTransactions, isEmpty);
    });
  });

  group('RecurringPayment Model Tests', () {
    test('RecurringPayment should parse correctly', () {
      final json = {
        'id': 1,
        'description': 'Rent',
        'amount': 25000.0,
        'frequency': 'monthly',
        'next_due_date': '2026-03-01',
        'counterparty': 'Landlord',
        'currency': 'RUB',
      };

      final payment = RecurringPayment.fromJson(json);

      expect(payment.id, 1);
      expect(payment.description, 'Rent');
      expect(payment.amount, 25000.0);
      expect(payment.frequency, 'monthly');
      expect(payment.nextDueDate, '2026-03-01');
      expect(payment.counterparty, 'Landlord');
    });

    test('RecurringPayment.isDueSoon should work correctly', () {
      final now = DateTime.now();

      // Create payment due in 3 days
      final dueDate = now.add(const Duration(days: 3));
      final paymentJson = {
        'id': 1,
        'description': 'Test',
        'amount': 100.0,
        'frequency': 'monthly',
        'next_due_date': dueDate.toIso8601String().split('T')[0],
      };

      final payment = RecurringPayment.fromJson(paymentJson);

      expect(payment.isDueSoon, true);
      expect(payment.isOverdue, false);
    });

    test('RecurringPayment.isOverdue should work correctly', () {
      final now = DateTime.now();

      // Create payment overdue by 1 day
      final dueDate = now.subtract(const Duration(days: 1));
      final paymentJson = {
        'id': 1,
        'description': 'Test',
        'amount': 100.0,
        'frequency': 'monthly',
        'next_due_date': dueDate.toIso8601String().split('T')[0],
      };

      final payment = RecurringPayment.fromJson(paymentJson);

      expect(payment.isOverdue, true);
      expect(payment.isDueSoon, false);
    });

    test('RecurringPayment.frequencyDisplay should return correct label', () {
      final monthlyPayment = RecurringPayment.fromJson({
        'id': 1,
        'description': 'Test',
        'amount': 100.0,
        'frequency': 'monthly',
        'next_due_date': '2026-03-01',
      });

      final weeklyPayment = RecurringPayment.fromJson({
        'id': 2,
        'description': 'Test',
        'amount': 100.0,
        'frequency': 'weekly',
        'next_due_date': '2026-03-01',
      });

      expect(monthlyPayment.frequencyDisplay, 'Ежемесячно');
      expect(weeklyPayment.frequencyDisplay, 'Еженедельно');
    });
  });
}
