import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:animations/animations.dart';
import 'package:shared_preferences/shared_preferences.dart';

import '../services/theme_service.dart';
import '../services/auto_refresh_service.dart';
import '../services/crypto_api_service.dart';

class CryptoSettingsScreen extends StatelessWidget {
  const CryptoSettingsScreen({super.key});

  @override
  Widget build(BuildContext context) {
    final themeService = Provider.of<ThemeService>(context);
    final refreshService = Provider.of<AutoRefreshService>(context);

    return Scaffold(
      appBar: AppBar(
        title: const Text('Настройки'),
        actions: [
          IconButton(
            icon: const Icon(Icons.info_outline),
            onPressed: () {
              _showAboutDialog(context);
            },
          ),
        ],
      ),
      body: ListView(
        children: [
          _buildSectionHeader('Внешний вид'),
          _buildThemeSelector(context, themeService),
          const Divider(height: 32),
          _buildSectionHeader('Автообновление'),
          _buildAutoRefreshSettings(context, refreshService),
          const Divider(height: 32),
          _buildSectionHeader('Приложение'),
          _buildLogoutTile(context),
          _buildAppInfoTile(context),
          const SizedBox(height: 16),
        ],
      ),
    );
  }

  Widget _buildSectionHeader(String title) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
      child: Text(
        title.toUpperCase(),
        style: const TextStyle(
          fontSize: 12,
          fontWeight: FontWeight.bold,
          letterSpacing: 1.2,
        ),
      ),
    );
  }

  Widget _buildAutoRefreshSettings(
    BuildContext context,
    AutoRefreshService refreshService,
  ) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: Column(
        children: [
          SwitchListTile(
            title: const Text('Автообновление'),
            subtitle: Text(
              refreshService.isAutoRefreshEnabled
                  ? 'Обновление каждые ${refreshService.refreshInterval} сек'
                  : 'Отключено',
            ),
            secondary: Icon(
              refreshService.isAutoRefreshEnabled
                  ? Icons.sync
                  : Icons.sync_disabled,
              color: refreshService.isAutoRefreshEnabled
                  ? Theme.of(context).colorScheme.primary
                  : Colors.grey,
            ),
            value: refreshService.isAutoRefreshEnabled,
            onChanged: (value) {
              refreshService.setAutoRefresh(value);
            },
          ),
          if (refreshService.isAutoRefreshEnabled) ...[
            const Divider(height: 1),
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text('Интервал обновления'),
                      Text(
                        '${refreshService.refreshInterval} сек',
                        style: TextStyle(
                          color: Theme.of(context).colorScheme.primary,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  Slider(
                    value: refreshService.refreshInterval.toDouble(),
                    min: 15,
                    max: 300,
                    divisions: 19,
                    label: '${refreshService.refreshInterval} сек',
                    onChanged: (value) {
                      refreshService.setRefreshInterval(value.toInt());
                    },
                  ),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text('15 сек', style: TextStyle(fontSize: 12)),
                      const Text('1 мин', style: TextStyle(fontSize: 12)),
                      const Text('3 мин', style: TextStyle(fontSize: 12)),
                      const Text('5 мин', style: TextStyle(fontSize: 12)),
                    ],
                  ),
                ],
              ),
            ),
          ],
        ],
      ),
    );
  }

  Widget _buildThemeSelector(BuildContext context, ThemeService themeService) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: Column(
        children: [
          _buildThemeOption(
            context: context,
            title: 'Светлая тема',
            subtitle: 'Всегда использовать светлую тему',
            icon: Icons.light_mode,
            themeMode: ThemeMode.light,
            selectedMode: themeService.themeMode,
          ),
          const Divider(height: 1),
          _buildThemeOption(
            context: context,
            title: 'Темная тема',
            subtitle: 'Всегда использовать темную тему',
            icon: Icons.dark_mode,
            themeMode: ThemeMode.dark,
            selectedMode: themeService.themeMode,
          ),
          const Divider(height: 1),
          _buildThemeOption(
            context: context,
            title: 'Системная тема',
            subtitle: 'Следовать настройкам системы',
            icon: Icons.brightness_auto,
            themeMode: ThemeMode.system,
            selectedMode: themeService.themeMode,
          ),
        ],
      ),
    );
  }

  Widget _buildThemeOption({
    required BuildContext context,
    required String title,
    required String subtitle,
    required IconData icon,
    required ThemeMode themeMode,
    required ThemeMode selectedMode,
  }) {
    final isSelected = themeMode == selectedMode;

    return OpenContainer(
      closedElevation: 0,
      closedColor: Colors.transparent,
      openColor: Theme.of(context).cardColor,
      transitionDuration: const Duration(milliseconds: 300),
      openBuilder: (context, action) {
        return const SizedBox();
      },
      closedBuilder: (context, action) {
        return ListTile(
          contentPadding: const EdgeInsets.symmetric(horizontal: 20, vertical: 8),
          leading: Container(
            padding: const EdgeInsets.all(8),
            decoration: BoxDecoration(
              color: isSelected
                  ? Theme.of(context).colorScheme.primary.withOpacity(0.1)
                  : Colors.transparent,
              borderRadius: BorderRadius.circular(12),
            ),
            child: Icon(
              icon,
              color: isSelected
                  ? Theme.of(context).colorScheme.primary
                  : Theme.of(context).iconTheme.color,
            ),
          ),
          title: Text(
            title,
            style: TextStyle(
              fontWeight: isSelected ? FontWeight.bold : FontWeight.normal,
              color: isSelected
                  ? Theme.of(context).colorScheme.primary
                  : Theme.of(context).textTheme.titleMedium?.color,
            ),
          ),
          subtitle: Text(
            subtitle,
            style: TextStyle(
              fontSize: 12,
              color: Theme.of(context).textTheme.bodySmall?.color,
            ),
          ),
          trailing: isSelected
              ? Icon(
                  Icons.check_circle,
                  color: Theme.of(context).colorScheme.primary,
                )
              : null,
        );
      },
      tappable: true,
    );
  }

  Widget _buildLogoutTile(BuildContext context) {
    final apiService = Provider.of<CryptoApiService>(context);
    
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: ListTile(
        contentPadding: const EdgeInsets.all(16),
        leading: Container(
          padding: const EdgeInsets.all(8),
          decoration: BoxDecoration(
            color: Colors.red.withOpacity(0.1),
            borderRadius: BorderRadius.circular(12),
          ),
          child: const Icon(Icons.logout, color: Colors.red),
        ),
        title: const Text(
          'Выйти',
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
        subtitle: const Text('Выход из аккаунта'),
        onTap: () async {
          final confirm = await showDialog<bool>(
            context: context,
            builder: (context) => AlertDialog(
              title: const Text('Выйти?'),
              content: const Text('Вы уверены что хотите выйти из аккаунта?'),
              actions: [
                TextButton(
                  onPressed: () => Navigator.pop(context, false),
                  child: const Text('Отмена'),
                ),
                TextButton(
                  onPressed: () => Navigator.pop(context, true),
                  child: const Text('Выйти'),
                ),
              ],
            ),
          );
          
          if (confirm == true) {
            final prefs = await SharedPreferences.getInstance();
            await prefs.remove('crypto_session_cookie');
            apiService.setSessionCookie(null);
            if (context.mounted) {
              Navigator.of(context).pushNamedAndRemoveUntil('/', (route) => false);
            }
          }
        },
      ),
    );
  }

  Widget _buildAppInfoTile(BuildContext context) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: Column(
        children: [
          ListTile(
            contentPadding: const EdgeInsets.all(20),
            leading: const Icon(Icons.currency_bitcoin, color: Colors.orange),
            title: const Text(
              'Zhamlik Crypto',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            subtitle: const Text('Версия 1.0.0'),
          ),
          ListTile(
            contentPadding: const EdgeInsets.fromLTRB(20, 0, 20, 20),
            leading: const Icon(Icons.trending_up, color: Colors.green),
            title: const Text('Управление криптопортфелем'),
            subtitle: const Text('© 2026 Zhamlik'),
          ),
        ],
      ),
    );
  }

  void _showAboutDialog(BuildContext context) {
    showAboutDialog(
      context: context,
      applicationName: 'Zhamlik Crypto',
      applicationVersion: '1.0.0',
      applicationLegalese: '© 2026 Zhamlik. Все права защищены.',
      children: [
        const SizedBox(height: 16),
        const Text(
          'Умное управление криптоинвестициями и портфелем.',
        ),
      ],
    );
  }
}
