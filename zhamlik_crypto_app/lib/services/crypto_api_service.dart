import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';
import 'package:shared_preferences/shared_preferences.dart';

import '../models/crypto_asset.dart';
import '../models/crypto_platform.dart';
import '../models/crypto_overview.dart';
import '../models/futures_position.dart';
import '../models/notification.dart';

class CryptoApiService extends ChangeNotifier {
  // Use main server (port 80) instead of proxy
  static String get baseUrl => 'http://193.29.224.20';
  
  String? _sessionCookie;

  String? get token => _token;
  bool get isAuthenticated => _sessionCookie != null;
  
  String? _token;

  void setToken(String? token) {
    _token = token;
    notifyListeners();
  }
  
  Future<bool> login(String username, String password) async {
    try {
      final response = await _post(
        '/api/mobile/auth/login',
        body: {'username': username, 'password': password},
        withCookie: false,
      );
      
      if (response.statusCode == 200) {
        _updateCookies(response);
        return true;
      }
      return false;
    } catch (e) {
      debugPrint('Login error: $e');
      return false;
    }
  }
  
  void setSessionCookie(String? cookie) {
    _sessionCookie = cookie;
    notifyListeners();
  }

  Future<void> loadSession() async {
    final prefs = await SharedPreferences.getInstance();
    _sessionCookie = prefs.getString('crypto_session_cookie');
  }

  Future<void> saveSession(String cookie) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString('crypto_session_cookie', cookie);
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
  
  void _updateCookies(http.Response response) {
    final setCookie = response.headers['set-cookie'];
    if (setCookie != null && setCookie.contains('session=')) {
      final sessionMatch = RegExp(r'session=([^;]+)').firstMatch(setCookie);
      if (sessionMatch != null) {
        _sessionCookie = 'session=${sessionMatch.group(1)}';
        saveSession(_sessionCookie!);
      }
    }
  }

  Future<http.Response> _get(String endpoint, {bool withCookie = true}) async {
    final url = Uri.parse('$baseUrl$endpoint');
    final headers = withCookie ? _headersWithCookie : _headers;
    return await http.get(url, headers: headers).timeout(const Duration(seconds: 30));
  }

  Future<http.Response> _post(String endpoint, {Map<String, dynamic>? body, bool withCookie = true}) async {
    final url = Uri.parse('$baseUrl$endpoint');
    final headers = withCookie ? _headersWithCookie : _headers;
    return await http.post(
      url,
      headers: headers,
      body: body != null ? jsonEncode(body) : null,
    ).timeout(const Duration(seconds: 30));
  }

  // ==================== OVERVIEW ====================

  Future<CryptoOverview?> getOverview() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/overview');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        return CryptoOverview.fromJson(data);
      } else if (response.statusCode == 404) {
        debugPrint('Overview endpoint not found: 404');
        return null;
      } else if (response.statusCode >= 500) {
        debugPrint('Server error: ${response.statusCode}');
        return null;
      } else {
        debugPrint('Error fetching overview: ${response.statusCode}');
        return null;
      }
    } on http.ClientException catch (e) {
      debugPrint('Network exception fetching overview: $e');
      return null;
    } catch (e) {
      debugPrint('Exception fetching overview: $e');
      return null;
    }
  }

  // ==================== PLATFORMS ====================

  Future<List<CryptoPlatform>> getPlatforms() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/platforms');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final platformsJson = data['platforms'] as List;
        return platformsJson.map((json) => CryptoPlatform.fromJson(json)).toList();
      } else {
        debugPrint('Error fetching platforms: ${response.statusCode}');
        return [];
      }
    } catch (e) {
      debugPrint('Exception fetching platforms: $e');
      return [];
    }
  }

  Future<CryptoPlatform?> getPlatform(int id) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/platforms/$id');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        return CryptoPlatform.fromJson(data);
      } else {
        return null;
      }
    } catch (e) {
      debugPrint('Exception fetching platform: $e');
      return null;
    }
  }

  Future<Map<String, dynamic>?> createPlatform({
    required String name,
    String? apiKey,
    String? apiSecret,
    String? passphrase,
    String? notes,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post(
        '/api/mobile/crypto/platforms',
        body: {
          'name': name,
          if (apiKey != null) 'api_key': apiKey,
          if (apiSecret != null) 'api_secret': apiSecret,
          if (passphrase != null) 'passphrase': passphrase,
          if (notes != null) 'notes': notes,
        },
      );
      _updateCookies(response);

      if (response.statusCode == 200 || response.statusCode == 201) {
        return jsonDecode(response.body);
      } else {
        final error = jsonDecode(response.body);
        debugPrint('Error creating platform: ${error['message']}');
        return null;
      }
    } catch (e) {
      debugPrint('Exception creating platform: $e');
      return null;
    }
  }

  Future<Map<String, dynamic>?> syncPlatform(int platformId) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post('/api/mobile/crypto/platforms/$platformId/sync');
      _updateCookies(response);

      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      } else {
        final error = jsonDecode(response.body);
        return {'success': false, 'message': error['message']};
      }
    } catch (e) {
      debugPrint('Exception syncing platform: $e');
      return {'success': false, 'message': e.toString()};
    }
  }

  // ==================== ASSETS ====================

  Future<List<CryptoAsset>> getAssets({int? platformId}) async {
    try {
      if (_sessionCookie == null) await loadSession();
      
      List<CryptoAsset> allAssets = [];
      
      if (platformId != null) {
        // Get assets for specific platform
        final response = await _get('/api/mobile/crypto/platforms/$platformId');
        _updateCookies(response);

        if (response.statusCode == 200) {
          final data = jsonDecode(response.body);
          final assetsJson = data['assets'] as List? ?? [];
          return assetsJson.map((json) => CryptoAsset.fromJson(json)).toList();
        } else {
          return [];
        }
      } else {
        // Get all platforms and their assets
        final response = await _get('/api/mobile/crypto/platforms');
        _updateCookies(response);

        if (response.statusCode == 200) {
          final data = jsonDecode(response.body);
          final platforms = data['platforms'] as List? ?? [];
          
          for (var platform in platforms) {
            final assetsJson = platform['assets'] as List? ?? [];
            for (var assetJson in assetsJson) {
              allAssets.add(CryptoAsset.fromJson(assetJson));
            }
          }
          return allAssets;
        } else {
          return [];
        }
      }
    } catch (e) {
      debugPrint('Exception fetching assets: $e');
      return [];
    }
  }

  Future<Map<String, dynamic>?> createAsset({
    required int platformId,
    required String ticker,
    required double quantity,
    String? sourceAccountType,
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _post(
        '/api/mobile/crypto/assets',
        body: {
          'platform_id': platformId,
          'ticker': ticker.toLowerCase(),
          'quantity': quantity,
          if (sourceAccountType != null) 'source_account_type': sourceAccountType,
        },
      );
      _updateCookies(response);

      if (response.statusCode == 200 || response.statusCode == 201) {
        return jsonDecode(response.body);
      } else {
        final error = jsonDecode(response.body);
        return {'success': false, 'message': error['message']};
      }
    } catch (e) {
      debugPrint('Exception creating asset: $e');
      return {'success': false, 'message': e.toString()};
    }
  }

  // ==================== ANALYTICS ====================

  Future<Map<String, dynamic>?> getAnalytics({int days = 30}) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/analytics?days=$days');
      _updateCookies(response);

      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      } else {
        debugPrint('Error fetching analytics: ${response.statusCode}');
        return null;
      }
    } catch (e) {
      debugPrint('Exception fetching analytics: $e');
      return null;
    }
  }

  // ==================== TRANSACTIONS ====================

  Future<Map<String, dynamic>?> getTransactions({
    int page = 1,
    String filterType = 'all',
  }) async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/transactions?page=$page&filter_type=$filterType');
      _updateCookies(response);

      if (response.statusCode == 200) {
        return jsonDecode(response.body);
      } else {
        debugPrint('Error fetching transactions: ${response.statusCode}');
        return null;
      }
    } catch (e) {
      debugPrint('Exception fetching transactions: $e');
      return null;
    }
  }

  // ==================== FUTURES ====================

  Future<FuturesOverview?> getFuturesOverview() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/futures/overview');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        if (data['success'] == true) {
          return FuturesOverview.fromJson(data);
        }
        return null;
      } else if (response.statusCode == 404) {
        debugPrint('Futures overview endpoint not found: 404');
        return null;
      }
      debugPrint('Error fetching futures overview: ${response.statusCode}');
      return null;
    } catch (e) {
      debugPrint('Exception fetching futures overview: $e');
      return null;
    }
  }

  Future<List<FuturesPosition>> getFuturesPositions() async {
    try {
      if (_sessionCookie == null) await loadSession();
      final response = await _get('/api/mobile/crypto/futures/positions');
      _updateCookies(response);

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        if (data['success'] == true && data['positions'] != null) {
          return (data['positions'] as List)
              .map((e) => FuturesPosition.fromJson(e as Map<String, dynamic>))
              .toList();
        }
        return [];
      }
      debugPrint('Error fetching futures positions: ${response.statusCode}');
      return [];
    } catch (e) {
      debugPrint('Exception fetching futures positions: $e');
      return [];
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
