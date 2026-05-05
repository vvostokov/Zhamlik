import 'package:flutter/foundation.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';

import 'api_service.dart';

class AuthService extends ChangeNotifier {
  static const String _userKey = 'user_data';
  static const String _sessionCookieKey = 'session_cookie';

  Map<String, dynamic>? _user;
  String? _sessionCookie;

  Map<String, dynamic>? get user => _user;
  bool get isAuthenticated => _sessionCookie != null;

  ApiService? _apiService;

  void setApiService(ApiService apiService) {
    _apiService = apiService;
  }

  // Load saved session from storage
  Future<void> loadAuthData() async {
    final prefs = await SharedPreferences.getInstance();
    _sessionCookie = prefs.getString(_sessionCookieKey);
    final userString = prefs.getString(_userKey);
    if (userString != null) {
      _user = jsonDecode(userString);
    }

    if (_apiService != null && _sessionCookie != null) {
      _apiService!.setSessionCookie(_sessionCookie);
    }

    notifyListeners();
  }

  // Save session to storage
  Future<void> _saveAuthData(String sessionCookie, Map<String, dynamic> user) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(_sessionCookieKey, sessionCookie);
    await prefs.setString(_userKey, jsonEncode(user));
  }

  // Login - handles Flask-Login session cookies
  Future<bool> login(String username, String password) async {
    try {
      final url = Uri.parse('${ApiService.baseUrl}/api/mobile/auth/login');
      final client = http.Client();
      
      final response = await client.post(
        url,
        headers: {'Content-Type': 'application/json'},
        body: jsonEncode({
          'username': username,
          'password': password,
        }),
      ).timeout(const Duration(seconds: 10));

      // Extract session cookie from response
      final setCookie = response.headers['set-cookie'];
      if (setCookie != null && setCookie.contains('session=')) {
        final sessionMatch = RegExp(r'session=([^;]+)').firstMatch(setCookie);
        if (sessionMatch != null) {
          _sessionCookie = 'session=${sessionMatch.group(1)}';
        }
      }

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        if (data['success'] == true) {
          _user = data['user'];

          if (_sessionCookie != null) {
            await _saveAuthData(_sessionCookie!, _user!);
            if (_apiService != null) {
              _apiService!.setSessionCookie(_sessionCookie);
            }
          }

          notifyListeners();
          return true;
        }
      }

      return false;
    } catch (e) {
      debugPrint('Login error: $e');
      return false;
    }
  }

  // Logout
  Future<void> logout() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_sessionCookieKey);
    await prefs.remove(_userKey);

    _sessionCookie = null;
    _user = null;

    if (_apiService != null) {
      _apiService!.setSessionCookie(null);
    }

    notifyListeners();
  }
}
