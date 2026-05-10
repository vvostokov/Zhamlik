import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:url_launcher/url_launcher.dart';

class AppVersion {
  final String version;
  final int versionCode;
  final String apkUrl;
  final bool forceUpdate;
  final List<String> changes;

  AppVersion({
    required this.version,
    required this.versionCode,
    required this.apkUrl,
    required this.forceUpdate,
    required this.changes,
  });

  factory AppVersion.fromJson(Map<String, dynamic> json) {
    return AppVersion(
      version: json['version'] as String,
      versionCode: json['version_code'] as int,
      apkUrl: json['apk_url'] as String,
      forceUpdate: json['force_update'] as bool? ?? false,
      changes: (json['changes'] as List<dynamic>?)
              ?.map((e) => e as String)
              .toList() ??
          [],
    );
  }
}

class VersionService extends ChangeNotifier {
  static const String _baseUrl = 'https://193.29.224.20:8443';
  static const String _appType = 'banking';

  static const String _currentVersion = '1.3.0';
  static const int _currentVersionCode = 9;

  AppVersion? _latestVersion;
  bool _isLoading = false;
  bool _hasError = false;
  String? _errorMessage;

  AppVersion? get latestVersion => _latestVersion;
  bool get isLoading => _isLoading;
  bool get hasError => _hasError;
  String? get errorMessage => _errorMessage;

  bool get hasUpdate {
    if (_latestVersion == null) return false;
    return _latestVersion!.versionCode > _currentVersionCode;
  }

  bool get forceUpdate => _latestVersion?.forceUpdate ?? false;

  Future<void> checkForUpdates() async {
    _isLoading = true;
    _hasError = false;
    _errorMessage = null;
    notifyListeners();

    try {
      final response = await http.get(
        Uri.parse('$_baseUrl/api/mobile/versions'),
      ).timeout(const Duration(seconds: 10));

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);

        if (data['success'] == true && data['versions'] != null) {
          final versionData = data['versions'][_appType];
          if (versionData != null) {
            _latestVersion = AppVersion.fromJson(versionData);
          }
        }
      } else {
        _hasError = true;
        _errorMessage = 'Ошибка проверки обновлений';
      }
    } catch (e) {
      _hasError = true;
      _errorMessage = 'Нет подключения к серверу';
      debugPrint('Error checking for updates: $e');
    } finally {
      _isLoading = false;
      notifyListeners();
    }
  }

  Future<void> downloadAndInstallUpdate() async {
    if (_latestVersion == null) return;

    final url = _latestVersion!.apkUrl;
    final uri = Uri.parse(url);

    // Try launching directly (bypass canLaunchUrl check for HTTP)
    try {
      final launched = await launchUrl(
        uri,
        mode: LaunchMode.externalApplication,
      );
      if (!launched) {
        throw Exception('Браузер не смог открыть ссылку');
      }
    } catch (e) {
      debugPrint('Error launching URL: $e');
      throw Exception('Не удалось открыть ссылку: $url');
    }
  }

  Future<void> dismissUpdate() async {
    final prefs = await SharedPreferences.getInstance();
    if (_latestVersion != null) {
      await prefs.setInt('dismissed_version', _latestVersion!.versionCode);
    }
  }

  Future<bool> isUpdateDismissed() async {
    final prefs = await SharedPreferences.getInstance();
    final dismissed = prefs.getInt('dismissed_version') ?? 0;
    return _latestVersion != null && dismissed >= _latestVersion!.versionCode;
  }

  static Future<void> showUpdateDialogIfNeeded(
    BuildContext context,
    VersionService versionService,
  ) async {
    // Ждем завершения проверки
    while (versionService.isLoading) {
      await Future.delayed(const Duration(milliseconds: 100));
    }

    if (!versionService.hasUpdate) return;

    // Если обновление отклонено и не принудительное
    if (await versionService.isUpdateDismissed() && !versionService.forceUpdate) {
      return;
    }

    if (!context.mounted) return;

    showDialog(
      context: context,
      barrierDismissible: !versionService.forceUpdate,
      builder: (context) => AlertDialog(
        title: Row(
          children: [
            Icon(
              Icons.system_update,
              color: Theme.of(context).primaryColor,
            ),
            const SizedBox(width: 8),
            const Text('Доступно обновление'),
          ],
        ),
        content: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                'Версия ${versionService.latestVersion!.version} доступна!',
                style: Theme.of(context).textTheme.titleMedium,
              ),
              const SizedBox(height: 16),
              const Text(
                'Что нового:',
                style: TextStyle(fontWeight: FontWeight.bold),
              ),
              const SizedBox(height: 8),
              ...versionService.latestVersion!.changes.map(
                (change) => Padding(
                  padding: const EdgeInsets.only(left: 16, bottom: 4),
                  child: Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('• '),
                      Expanded(child: Text(change)),
                    ],
                  ),
                ),
              ),
            ],
          ),
        ),
        actions: [
          if (!versionService.forceUpdate)
            TextButton(
              onPressed: () {
                versionService.dismissUpdate();
                Navigator.of(context).pop();
              },
              child: const Text('Позже'),
            ),
          ElevatedButton(
            onPressed: () async {
              try {
                await versionService.downloadAndInstallUpdate();
              } catch (e) {
                if (context.mounted) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(
                      content: Text('Ошибка: $e'),
                      backgroundColor: Colors.red,
                    ),
                  );
                }
              }
            },
            child: const Text('Обновить'),
          ),
        ],
      ),
    );
  }
}
