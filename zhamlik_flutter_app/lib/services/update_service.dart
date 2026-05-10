import 'dart:io';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:path_provider/path_provider.dart';
import 'package:open_file/open_file.dart';
import 'package:permission_handler/permission_handler.dart';

class UpdateService {
  static const String _updateServerUrl = 'https://193.29.224.20:8443/api/mobile';
  static const String _appName = 'banking';

  // Current app version
  static const String _currentVersion = '1.3.0';
  static const int _currentVersionCode = 9;

  // Public getter for version display
  static String get currentVersion => _currentVersion;

  Future<UpdateInfo?> checkForUpdates() async {
    try {
      final response = await http.get(
        Uri.parse('$_updateServerUrl/versions'),
      ).timeout(const Duration(seconds: 10));

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        if (data['success'] == true && data['versions'] != null) {
          final versionData = data['versions'][_appName];
          if (versionData != null) {
            final serverVersionCode = versionData['version_code'] as int? ?? 0;

            if (serverVersionCode > _currentVersionCode) {
              return UpdateInfo(
                version: versionData['version'] as String,
                versionCode: serverVersionCode,
                downloadUrl: versionData['apk_url'] as String,
                releaseDate: DateTime.now().toString().split(' ')[0],
                changes: (versionData['changes'] as List<dynamic>?)
                        ?.map((e) => e as String)
                        .toList() ??
                    [],
                forceUpdate: versionData['force_update'] as bool? ?? false,
              );
            }
          }
        }
      }
      return null;
    } catch (e) {
      debugPrint('Error checking for updates: $e');
      return null;
    }
  }

  Future<String?> _downloadUpdate(String downloadUrl) async {
    try {
      // Request storage permission
      final status = await Permission.storage.request();
      if (!status.isGranted) {
        throw Exception('Storage permission not granted');
      }

      // Download APK
      final response = await http.get(Uri.parse(downloadUrl));
      
      // Get downloads directory
      final directory = await getDownloadsDirectory();
      if (directory == null) {
        throw Exception('Downloads directory not available');
      }

      final apkPath = '${directory.path}/${_appName}_update.apk';
      final file = File(apkPath);
      await file.writeAsBytes(response.bodyBytes);

      return apkPath;
    } catch (e) {
      debugPrint('Error downloading update: $e');
      return null;
    }
  }

  Future<bool> installUpdate(String apkPath) async {
    try {
      // Request install permission
      final status = await Permission.requestInstallPackages.request();
      if (!status.isGranted) {
        throw Exception('Install permission not granted');
      }

      // Open APK file to trigger installation
      final result = await OpenFile.open(apkPath, type: 'application/vnd.android.package-archive');
      
      return result.type == ResultType.done;
    } catch (e) {
      debugPrint('Error installing update: $e');
      return false;
    }
  }

  Future<void> performUpdateIfNeeded(BuildContext context) async {
    final updateInfo = await checkForUpdates();
    
    if (updateInfo != null) {
      // Show update dialog
      if (context.mounted) {
        _showUpdateDialog(context, updateInfo);
      }
    }
  }

  void _showUpdateDialog(BuildContext context, UpdateInfo updateInfo) {
    showDialog(
      context: context,
      barrierDismissible: updateInfo.forceUpdate == false,
      builder: (context) => AlertDialog(
        title: Row(
          children: [
            const Icon(Icons.system_update, color: Colors.orange),
            const SizedBox(width: 12),
            Expanded(
              child: Text(
                'Доступно обновление',
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
          ],
        ),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Версия ${updateInfo.version}',
              style: const TextStyle(
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
            ),
            const SizedBox(height: 8),
            Text(
              'Дата выхода: ${updateInfo.releaseDate}',
              style: TextStyle(
                color: Colors.grey[600],
                fontSize: 12,
              ),
            ),
            const SizedBox(height: 16),
            const Text(
              'Что нового:',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 8),
            ...updateInfo.changes.map((change) => Padding(
              padding: const EdgeInsets.only(left: 16, bottom: 4),
              child: Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  const Text('• ', style: TextStyle(fontWeight: FontWeight.bold)),
                  Expanded(child: Text(change)),
                ],
              ),
            )),
          ],
        ),
        actions: [
          if (!updateInfo.forceUpdate)
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Позже'),
            ),
          ElevatedButton(
            onPressed: () async {
              Navigator.of(context).pop();
              await _downloadAndUpdate(context, updateInfo);
            },
            style: ElevatedButton.styleFrom(
              backgroundColor: Colors.orange,
              foregroundColor: Colors.white,
            ),
            child: const Text('Обновить'),
          ),
        ],
      ),
    );
  }

  Future<void> _downloadAndUpdate(BuildContext context, UpdateInfo updateInfo) async {
    // Show download progress
    if (!context.mounted) return;
    
    showDialog(
      context: context,
      barrierDismissible: false,
      builder: (context) => const AlertDialog(
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            CircularProgressIndicator(),
            SizedBox(height: 16),
            Text('Загрузка обновления...'),
          ],
        ),
      ),
    );

    // Download update
    final apkPath = await _downloadUpdate(updateInfo.downloadUrl);
    
    if (!context.mounted) return;
    Navigator.of(context).pop();

    if (apkPath != null) {
      // Show install dialog
      showDialog(
        context: context,
        barrierDismissible: false,
        builder: (context) => AlertDialog(
          title: const Text('Обновление загружено'),
          content: const Text('Файл обновления скачан. Установить теперь?'),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Отмена'),
            ),
            ElevatedButton(
              onPressed: () async {
                Navigator.of(context).pop();
                await installUpdate(apkPath);
              },
              child: const Text('Установить'),
            ),
          ],
        ),
      );
    } else {
      if (context.mounted) {
        showDialog(
          context: context,
          builder: (context) => AlertDialog(
            title: const Text('Ошибка'),
            content: const Text('Не удалось загрузить обновление'),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(context).pop(),
                child: const Text('OK'),
              ),
            ],
          ),
        );
      }
    }
  }
}

class UpdateInfo {
  final String version;
  final int versionCode;
  final String downloadUrl;
  final String releaseDate;
  final List<String> changes;
  final bool forceUpdate;

  UpdateInfo({
    required this.version,
    required this.versionCode,
    required this.downloadUrl,
    required this.releaseDate,
    required this.changes,
    required this.forceUpdate,
  });

  factory UpdateInfo.fromJson(Map<String, dynamic> json) {
    return UpdateInfo(
      version: json['version'] as String,
      versionCode: json['version_code'] as int,
      downloadUrl: json['download_url'] as String,
      releaseDate: json['release_date'] as String,
      changes: List<String>.from(json['changes'] as List),
      forceUpdate: json['force_update'] as bool? ?? false,
    );
  }
}
