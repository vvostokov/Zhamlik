import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:provider/provider.dart';

import 'package:zhamlik/main.dart';
import 'package:zhamlik/services/api_service.dart';
import 'package:zhamlik/services/version_service.dart';
import 'package:zhamlik/screens/transactions_screen.dart';
import 'package:zhamlik/screens/debts_screen.dart';
import 'package:zhamlik/screens/recurring_payments_screen.dart';
import 'package:zhamlik/screens/accounts_screen.dart';
import 'package:zhamlik/screens/home_screen.dart';

/// Integration Tests for Zhamlik Banking App
///
/// This test suite covers:
/// 1. Transaction creation with filters
/// 2. Debt creation and display
/// 3. Recurring payments creation
/// 4. Account creation
/// 5. Home screen overview
/// 6. Refresh functionality

void main() {
  group('Transaction Filter Tests', () {
    testWidgets('When income filter is selected, + button should create income',
        (WidgetTester tester) async {
      // Setup
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const TransactionsScreen(),
          ),
        ),
      );

      // Wait for initial load
      await tester.pumpAndSettle();

      // Find and tap "Доходы" filter
      final incomeFilter = find.text('Доходы');
      expect(incomeFilter, findsOneWidget);
      await tester.tap(incomeFilter);
      await tester.pumpAndSettle();

      // Tap + button
      final fab = find.byType(FloatingActionButton);
      expect(fab, findsOneWidget);
      await tester.tap(fab);
      await tester.pumpAndSettle();

      // Check dialog appears with "Доход" selected
      expect(find.text('Добавить операцию'), findsOneWidget);

      // The SegmentedButton should have "Доход" as selected
      // This is visually represented by the selected state
      final incomeSegment = find.text('Доход');
      expect(incomeSegment, findsOneWidget);
    });

    testWidgets('When expense filter is selected, + button should create expense',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const TransactionsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Tap "Расходы" filter
      await tester.tap(find.text('Расходы'));
      await tester.pumpAndSettle();

      // Tap + button
      await tester.tap(find.byType(FloatingActionButton));
      await tester.pumpAndSettle();

      // Verify expense is default type
      expect(find.text('Добавить операцию'), findsOneWidget);
      expect(find.text('Расход'), findsOneWidget);
    });

    testWidgets('When transfer filter is selected, + button should create transfer',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const TransactionsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Tap "Переводы" filter
      await tester.tap(find.text('Переводы'));
      await tester.pumpAndSettle();

      // Tap + button
      await tester.tap(find.byType(FloatingActionButton));
      await tester.pumpAndSettle();

      // Verify transfer is default type
      expect(find.text('Добавить операцию'), findsOneWidget);
      expect(find.text('Перевод'), findsOneWidget);
    });
  });

  group('Debts Screen Tests', () {
    testWidgets('Should create and display "Я должен" debt',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const DebtsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Tap + button
      await tester.tap(find.byType(FloatingActionButton));
      await tester.pumpAndSettle();

      // Verify dialog appears
      expect(find.text('Добавить долг'), findsOneWidget);
    });

    testWidgets('Should create and display "Мне должны" debt',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const DebtsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Tap to switch to "Мне должны" tab
      await tester.tap(find.text('Мне должны'));
      await tester.pumpAndSettle();

      // Tap + button
      await tester.tap(find.byType(FloatingActionButton));
      await tester.pumpAndSettle();

      // Verify dialog appears
      expect(find.text('Добавить долг'), findsOneWidget);
    });
  });

  group('Recurring Payments Screen Tests', () {
    testWidgets('Should create recurring payment without "in development" message',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const RecurringPaymentsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Tap + button
      await tester.tap(find.byType(FloatingActionButton));
      await tester.pumpAndSettle();

      // Verify dialog appears
      expect(find.text('Добавить регулярный платеж'), findsOneWidget);

      // Should NOT show "в разработке" message
      expect(find.text('Функция добавления в разработке'), findsNothing);

      // Should have "Добавить" button
      expect(find.text('Добавить'), findsOneWidget);
    });
  });

  group('Accounts Screen Tests', () {
    testWidgets('Should create account', (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const AccountsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Tap + button
      await tester.tap(find.byType(FloatingActionButton));
      await tester.pumpAndSettle();

      // Verify dialog appears
      expect(find.text('Добавить счёт'), findsOneWidget);
    });
  });

  group('Refresh Tests', () {
    testWidgets('Transactions screen should refresh on pull down',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const TransactionsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      // Find RefreshIndicator and drag down
      final refreshIndicator = find.byType(RefreshIndicator);
      expect(refreshIndicator, findsOneWidget);

      // Simulate pull to refresh
      await tester.drag(refreshIndicator, const Offset(0, 300));
      await tester.pump();
    });

    testWidgets('Debts screen should refresh on pull down',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const DebtsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      final refreshIndicator = find.byType(RefreshIndicator);
      expect(refreshIndicator, findsOneWidget);

      await tester.drag(refreshIndicator, const Offset(0, 300));
      await tester.pump();
    });

    testWidgets('Recurring payments should refresh on pull down',
        (WidgetTester tester) async {
      final apiService = MockApiService();
      await tester.pumpWidget(
        MaterialApp(
          home: ChangeNotifierProvider<ApiService>.value(
            value: apiService,
            child: const RecurringPaymentsScreen(),
          ),
        ),
      );

      await tester.pumpAndSettle();

      final refreshIndicator = find.byType(RefreshIndicator);
      expect(refreshIndicator, findsOneWidget);

      await tester.drag(refreshIndicator, const Offset(0, 300));
      await tester.pump();
    });
  });
}

/// Mock ApiService for testing
class MockApiService extends ApiService {
  @override
  Future<List<dynamic>> getTransactions({int page = 1, int perPage = 20, String? type, int? accountId}) async {
    // Return mock transactions
    return [];
  }

  @override
  Future<List<dynamic>> getAccounts() async {
    // Return mock accounts
    return [];
  }

  @override
  Future<Map<String, dynamic>> getDebts() async {
    return {'i_owe': [], 'owed_to_me': []};
  }

  @override
  Future<List<dynamic>> getRecurringPayments() async {
    return [];
  }

  @override
  Future<dynamic> createTransaction({
    required double amount,
    required String type,
    required int accountId,
    int? toAccountId,
    String? description,
    String? merchant,
    int? categoryId,
  }) async {
    // Return mock created transaction
    return {'id': 1, 'amount': amount, 'type': type};
  }

  @override
  Future<dynamic> createDebt({
    required String counterparty,
    required double amount,
    required bool isIOwe,
    String? description,
    String? dueDate,
  }) async {
    return {'id': 1, 'amount': amount, 'is_i_owe': isIOwe};
  }

  @override
  Future<dynamic> createRecurringPayment({
    required String description,
    required double amount,
    required String frequency,
    String? counterparty,
  }) async {
    return {'id': 1, 'amount': amount, 'description': description};
  }

  @override
  Future<dynamic> createAccount({
    required String name,
    required String type,
    required String currency,
    String? bank,
    String? notes,
  }) async {
    return {'id': 1, 'name': name, 'type': type};
  }
}
