import 'dart:async';
import 'package:flutter/foundation.dart';
import 'crypto_api_service.dart';

/// Service for auto-refreshing crypto data
class AutoRefreshService extends ChangeNotifier {
  final CryptoApiService _apiService;
  Timer? _refreshTimer;
  bool _isAutoRefreshEnabled = false;
  int _refreshInterval = 60; // seconds
  DateTime? _lastRefreshTime;
  bool _isRefreshing = false;

  AutoRefreshService(this._apiService);

  // Getters
  bool get isAutoRefreshEnabled => _isAutoRefreshEnabled;
  int get refreshInterval => _refreshInterval;
  DateTime? get lastRefreshTime => _lastRefreshTime;
  bool get isRefreshing => _isRefreshing;

  /// Enable/disable auto-refresh
  Future<void> setAutoRefresh(bool enabled) async {
    if (_isAutoRefreshEnabled == enabled) return;

    _isAutoRefreshEnabled = enabled;

    if (enabled) {
      await _startRefreshTimer();
    } else {
      await _stopRefreshTimer();
    }

    notifyListeners();
  }

  /// Set refresh interval in seconds
  Future<void> setRefreshInterval(int seconds) async {
    if (_refreshInterval == seconds) return;

    _refreshInterval = seconds;

    // Restart timer if auto-refresh is enabled
    if (_isAutoRefreshEnabled) {
      await _stopRefreshTimer();
      await _startRefreshTimer();
    }

    notifyListeners();
  }

  /// Start the refresh timer
  Future<void> _startRefreshTimer() async {
    await _stopRefreshTimer();

    _refreshTimer = Timer.periodic(
      Duration(seconds: _refreshInterval),
      (_) => _refresh(),
    );

    // Initial refresh
    await _refresh();
  }

  /// Stop the refresh timer
  Future<void> _stopRefreshTimer() async {
    _refreshTimer?.cancel();
    _refreshTimer = null;
  }

  /// Perform refresh
  Future<void> _refresh() async {
    if (_isRefreshing) return;

    _isRefreshing = true;
    notifyListeners();

    try {
      // Refresh all data
      await _apiService.getOverview();
      await _apiService.getPlatforms();
      await _apiService.getAssets();

      _lastRefreshTime = DateTime.now();
      debugPrint('Auto-refresh completed at $_lastRefreshTime');
    } catch (e) {
      debugPrint('Auto-refresh error: $e');
    } finally {
      _isRefreshing = false;
      notifyListeners();
    }
  }

  /// Manual refresh
  Future<void> refresh() async {
    await _refresh();
  }

  @override
  void dispose() {
    _stopRefreshTimer();
    super.dispose();
  }
}
