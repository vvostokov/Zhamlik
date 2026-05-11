import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';
import 'package:shared_preferences/shared_preferences.dart';

import '../models/transaction.dart';
import '../models/account.dart';
import '../models/overview.dart';
import '../models/debt.dart';
import '../models/notification.dart';

class ApiService extends ChangeNotifier {
  // Use HTTP for testing (in production use HTTPS with valid SSL)
  static String get baseUrl {
    return 'http://193.29.224.20:8443';
  }
  
  String? _token;
  String? _sessionCookie;

  String? get token => _token;
  bool get isAuthenticated => _token != null;

  void setToken(String? token) {
    _token = token;
    notifyListeners();
  }

  void setSessionCookie(String? cookie) {
    _sessionCookie = cookie;
    notifyListeners();
  }

  // Load saved session from storage
  Future<void> loadSession() async {
    final prefs = await SharedPreferences.getInstance();
    _sessionCookie = prefs.getString('session_cookie');
  }

  // Save session to storage
  Future<void> saveSession(String cookie) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString('session_cookie', cookie);
    _sessionCookie = cookie;
  }

  Map<String, String> get _headers => {
    'Content-Type': 'application/json',
    if (_token != null) 'Authorization': 'Bearer $_token',
  };

  Map<String, String> get _headersWithCookie => {
    ..._headers,
    if (_sessionCookie != null) 'Cookie': _sessionCookie!,
  };

  Future<http.Response> _get(String endpoint, {bool withCookie = true}) async {
    final url = Uri.parse('$baseUrl$endpoint');
    final headers = withCookie ? _headersWithCookie : _headers;
    return await http.get(url, headers: headers).timeout(const Duration(seconds: 30));
  }

  Future<http.Response> _post(String endpoint, {Map<String, dynamic>? body, bool withCookie = true}) async {
    final url = Uri.parse('$baseUrl$endpoint');
    final headers = withCookie ? _headersWithCookie : _headers;
    debugPrint('[API] POST $endpoint with headers: $headers');
    debugPrint('[API] POST body: $body');
    final response = await http.post(
      url,
      headers: headers,
      body: body != null ? jsonEncode(body) : null,
    ).timeout(const Duration(seconds: 30));
    debugPrint('[API] POST response: ${response.statusCode}, body: ${response.body.substring(0, response.body.length > 500 ? 500 : response.body.length)}');
    return response;
  }

  // Store cookies from response
  void _updateCookies(http.Response response) {
    final setCookie = response.headers['set-cookie'];
    if (setCookie != null) {
      // Extract session cookie
      if (setCookie.contains('session=')) {
        final sessionMatch = RegExp(r'session=([^;]+)').firstMatch(setCookie);
        if (sessionMatch != null) {
          _sessionCookie = 'session=${sessionMatch.group(1)}';
          saveSession(_sessionCookie!);
        }
      }
    }
  }

  // ==================== OVERVIEW ====================

  Future<Overview?> getOverview() async {
    try {
      // Load session first if not loaded
      if (_sessionCookie == null) {
        await loadSession();
      }
      
      final response = await _get('/api/mobile/overview');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        return Overview.fromJson(data);
      } else if (response.statusCode == 401) {
        // Not authenticated, clear session
        _sessionCookie = null;
        notifyListeners();
        return null;
      } else {
        debugPrint('Error fetching overview: ${response.statusCode}');
        return null;
      }
    } catch (e) {
      debugPrint('Exception fetching overview: $e');
      return null;
    }
  }

  // ==================== ACCOUNTS ====================

  Future<List<Account>> getAccounts() async {
    try {
      if (_sessionCookie == null) {
        await loadSession();
      }
      
      final response = await _get('/api/mobile/accounts');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final accountsJson = data['accounts'] as List;
        return accountsJson.map((json) => Account.fromJson(json)).toList();
      } else {
        debugPrint('Error fetching accounts: ${response.statusCode}');
        return [];
      }
    } catch (e) {
      debugPrint('Exception fetching accounts: $e');
      return [];
    }
  }

  Future<Account?> getAccount(int id) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/accounts/$id');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        return Account.fromJson(data);
      } else {
        return null;
      }
    } catch (e) {
      debugPrint('Exception fetching account: $e');
      return null;
    }
  }

  Future<Account?> createAccount({
    required String name,
    required String type,
    required String currency,
    String? bank,
    String? notes,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post(
        '/api/mobile/accounts',
        body: {
          'name': name,
          'type': type,
          'currency': currency,
          if (bank != null) 'bank': bank,
          if (notes != null) 'notes': notes,
        },
      );
      _updateCookies(response);

      if (response.statusCode == 200 || response.statusCode == 201) {
        final data = jsonDecode(response.body);
        if (data['success'] == true && data['account'] != null) {
          return Account.fromJson(data['account']);
        }
      }
      debugPrint('Error creating account: ${response.statusCode}');
      return null;
    } catch (e) {
      debugPrint('Exception creating account: $e');
      return null;
    }
  }

  // ==================== TRANSACTIONS ====================

  Future<List<Transaction>> getTransactions({
    int page = 1,
    int perPage = 20,
    String? type,
    int? accountId,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      
      final queryParams = {
        'page': page.toString(),
        'per_page': perPage.toString(),
        if (type != null) 'type': type,
        if (accountId != null) 'account_id': accountId.toString(),
      };

      final queryString = queryParams.entries
          .map((e) => '${e.key}=${e.value}')
          .join('&');

      final response = await _get('/api/mobile/transactions?$queryString');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final transactionsJson = data['transactions'] as List;
        return transactionsJson.map((json) => Transaction.fromJson(json)).toList();
      } else {
        debugPrint('Error fetching transactions: ${response.statusCode}');
        return [];
      }
    } catch (e) {
      debugPrint('Exception fetching transactions: $e');
      return [];
    }
  }

  Future<Transaction?> createTransaction({
    required double amount,
    required String type,
    required int accountId,
    int? toAccountId,
    String? description,
    String? merchant,
    int? categoryId,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post(
        '/api/mobile/transactions',
        body: {
          'amount': amount,
          'transaction_type': type,
          'account_id': accountId,
          if (toAccountId != null) 'to_account_id': toAccountId,
          if (description != null) 'description': description,
          if (merchant != null) 'merchant': merchant,
          if (categoryId != null) 'category_id': categoryId,
          'date': DateTime.now().toIso8601String(),
        },
      );
      _updateCookies(response);

      if (response.statusCode == 201) {
        final data = jsonDecode(response.body);
        final transactionJson = data['transaction'];
        return Transaction.fromJson(transactionJson);
      } else {
        debugPrint('Error creating transaction: ${response.statusCode}');
        return null;
      }
    } catch (e) {
      debugPrint('Exception creating transaction: $e');
      return null;
    }
  }

  // ==================== QR PARSING ====================

  Future<Map<String, dynamic>?> parseQR(String qrString) async {
    debugPrint('[API] parseQR вызван с: $qrString');
    try {
      if (_sessionCookie == null) await loadSession();
      debugPrint('[API] Сессия: $_sessionCookie');
      final response = await _post(
        '/api/mobile/parse-qr',
        body: {'qr_string': qrString},
      );
      debugPrint('[API] parseQR ответ: status=${response.statusCode}, body=${response.body}');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        debugPrint('[API] parseQR успех: $data');
        return data;
      } else {
        final error = jsonDecode(response.body);
        debugPrint('[API] parseQR ошибка API: ${error['error']}');
        return {'error': error['error'] ?? 'Ошибка сервера'};
      }
    } catch (e) {
      debugPrint('[API] parseQR exception: $e');
      return null;
    }
  }

  Future<Map<String, dynamic>?> receiptToTransaction({
    required Map<String, dynamic> receiptData,
    required int accountId,
    int? categoryId,
  }) async {
    debugPrint('[API] receiptToTransaction START');
    debugPrint('[API] sessionCookie: $_sessionCookie');
    
    if (_sessionCookie == null) {
      debugPrint('[API] No session, loading...');
      await loadSession();
      debugPrint('[API] After loadSession, sessionCookie: $_sessionCookie');
    }
    
    debugPrint('[API] receiptData keys: ${receiptData.keys.toList()}');
    debugPrint('[API] accountId: $accountId');
    
    try {
      final response = await _post(
        '/api/mobile/receipt-to-transaction',
        body: {
          'receipt_data': receiptData,
          'account_id': accountId,
          if (categoryId != null) 'category_id': categoryId,
        },
      );
      debugPrint('[API] receiptToTransaction response: ${response.statusCode}');
      debugPrint('[API] response body: ${response.body}');
      _updateCookies(response);

      if (response.statusCode == 201) {
        return jsonDecode(response.body);
      } else if (response.statusCode == 404 || response.statusCode == 400 || response.statusCode == 500) {
        try {
          final error = jsonDecode(response.body);
          debugPrint('[API] Error response: ${response.statusCode}, error: ${error['error']}');
          return {'success': false, 'error': error['error'] ?? 'Ошибка сервера'};
        } catch (e) {
          debugPrint('[API] Failed to parse error response: ${response.body}');
          return {'success': false, 'error': 'Ошибка сервера: ${response.statusCode}'};
        }
      } else {
        debugPrint('[API] Unexpected status: ${response.statusCode}');
        return null;
      }
    } catch (e) {
      debugPrint('[API] Exception creating transaction from receipt: $e');
      return null;
    }
  }

  // ==================== CATEGORIES ====================

  Future<List<Map<String, dynamic>>> getCategories() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/categories');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        return List<Map<String, dynamic>>.from(data['categories']);
      } else {
        return [];
      }
    } catch (e) {
      debugPrint('Exception fetching categories: $e');
      return [];
    }
  }

  // ==================== ANALYTICS ====================

  Future<Map<String, dynamic>?> getAnalytics({int days = 30}) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/analytics?days=$days');
      _updateCookies(response);

      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      } else {
        return null;
      }
    } catch (e) {
      debugPrint('Exception fetching analytics: $e');
      return null;
    }
  }

  // ==================== DEBTS ====================

  Future<Map<String, dynamic>> getDebts() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/debts');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final iOweJson = data['i_owe'] as List? ?? [];
        final owedToMeJson = data['owed_to_me'] as List? ?? [];

        return {
          'i_owe': iOweJson.map((json) => Debt.fromJson(json)).toList(),
          'owed_to_me': owedToMeJson.map((json) => Debt.fromJson(json)).toList(),
        };
      } else {
        return {'i_owe': <Debt>[], 'owed_to_me': <Debt>[]};
      }
    } catch (e) {
      debugPrint('Exception fetching debts: $e');
      return {'i_owe': <Debt>[], 'owed_to_me': <Debt>[]};
    }
  }

  Future<Debt?> createDebt({
    required String counterparty,
    required double amount,
    required bool isIOwe,
    String? description,
    String? dueDate,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      // Convert isIOwe to debt_type
      final debtType = isIOwe ? 'i_owe' : 'owed_to_me';
      final response = await _post(
        '/api/mobile/debts',
        body: {
          'counterparty': counterparty,
          'amount': amount,
          'debt_type': debtType,
          if (description != null && description.isNotEmpty) 'description': description,
          if (dueDate != null) 'due_date': dueDate,
        },
      );
      _updateCookies(response);

      if (response.statusCode == 200 || response.statusCode == 201) {
        final data = jsonDecode(response.body);
        if (data['success'] == true && data['debt'] != null) {
          return Debt.fromJson(data['debt']);
        }
      }
      debugPrint('Error creating debt: ${response.statusCode}');
      return null;
    } catch (e) {
      debugPrint('Exception creating debt: $e');
      return null;
    }
  }

  // ==================== RECURRING PAYMENTS ====================

  Future<List<RecurringPayment>> getRecurringPayments() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/recurring-payments');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final paymentsJson = data['payments'] as List? ?? [];
        return paymentsJson.map((json) => RecurringPayment.fromJson(json)).toList();
      } else {
        return [];
      }
    } catch (e) {
      debugPrint('Exception fetching recurring payments: $e');
      return [];
    }
  }

  Future<RecurringPayment?> createRecurringPayment({
    required String description,
    required double amount,
    required String frequency,
    String? counterparty,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post(
        '/api/mobile/recurring-payments',
        body: {
          'description': description,
          'amount': amount,
          'frequency': frequency,
          if (counterparty != null && counterparty.isNotEmpty) 'counterparty': counterparty,
        },
      );
      _updateCookies(response);

      if (response.statusCode == 200 || response.statusCode == 201) {
        final data = jsonDecode(response.body);
        if (data['success'] == true && data['payment'] != null) {
          return RecurringPayment.fromJson(data['payment']);
        }
      }
      debugPrint('Error creating recurring payment: ${response.statusCode}');
      return null;
    } catch (e) {
      debugPrint('Exception creating recurring payment: $e');
      return null;
    }
  }

  // ==================== NOTIFICATIONS ====================

  Future<int> getUnreadNotificationsCount() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/notifications/unread');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        return data['count'] as int? ?? 0;
      }
      return 0;
    } catch (e) {
      debugPrint('Exception fetching notifications: $e');
      return 0;
    }
  }

  Future<List<AppNotification>> getNotifications() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/notifications');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        if (data['notifications'] != null) {
          return (data['notifications'] as List)
              .map((e) => AppNotification.fromJson(e as Map<String, dynamic>))
              .toList();
        }
        return [];
      }
      return [];
    } catch (e) {
      debugPrint('Exception fetching all notifications: $e');
      return [];
    }
  }

  Future<bool> markNotificationAsRead(int notificationId) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post('/api/notifications/$notificationId/read');
      _updateCookies(response);

      return response.statusCode == 200;
    } catch (e) {
      debugPrint('Exception marking notification read: $e');
      return false;
    }
  }
}
