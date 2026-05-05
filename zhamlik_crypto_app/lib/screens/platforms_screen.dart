import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/crypto_api_service.dart';
import '../models/crypto_platform.dart';
import '../models/popular_exchange.dart';

class PlatformsScreen extends StatefulWidget {
  const PlatformsScreen({super.key});

  @override
  State<PlatformsScreen> createState() => _PlatformsScreenState();
}

class _PlatformsScreenState extends State<PlatformsScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '\$',
    decimalDigits: 2,
  );

  List<CryptoPlatform> _platforms = [];
  bool _isLoading = true;
  final Map<int, bool> _syncing = {};

  @override
  void initState() {
    super.initState();
    _loadPlatforms();
  }

  Future<void> _loadPlatforms() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final platforms = await apiService.getPlatforms();

    if (mounted) {
      setState(() {
        _platforms = platforms;
        _isLoading = false;
      });
    }
  }

  Future<void> _syncPlatform(int platformId) async {
    setState(() {
      _syncing[platformId] = true;
    });

    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final result = await apiService.syncPlatform(platformId);

    if (mounted) {
      setState(() {
        _syncing[platformId] = false;
      });

      if (result != null && result['success'] == true) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(result['message'] ?? 'Синхронизация завершена'),
            backgroundColor: Colors.green,
          ),
        );
        _loadPlatforms();
      } else {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(result?['message'] ?? 'Ошибка синхронизации'),
            backgroundColor: Colors.red,
          ),
        );
      }
    }
  }

  double _getTotalValue() {
    return _platforms.fold(
      0.0,
      (sum, platform) => sum + (platform.totalValueUsd ?? 0.0),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Крипто-биржи'),
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
              onRefresh: _loadPlatforms,
              child: CustomScrollView(
                slivers: [
                  SliverToBoxAdapter(
                    child: Padding(
                      padding: const EdgeInsets.all(16),
                      child: Card(
                        child: Padding(
                          padding: const EdgeInsets.all(20),
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                'Общий баланс',
                                style: Theme.of(context)
                                    .textTheme
                                    .titleMedium
                                    ?.copyWith(color: Colors.grey[600]),
                              ),
                              const SizedBox(height: 8),
                              Text(
                                currencyFormat.format(_getTotalValue()),
                                style: Theme.of(context)
                                    .textTheme
                                    .headlineMedium
                                    ?.copyWith(
                                      color: Theme.of(context).colorScheme.primary,
                                      fontWeight: FontWeight.bold,
                                    ),
                              ),
                              const SizedBox(height: 8),
                              Text(
                                '${_platforms.length} бирж(ы)',
                                style: TextStyle(color: Colors.grey[600]),
                              ),
                            ],
                          ),
                        ),
                      ),
                    ),
                  ),
                  _platforms.isEmpty
                      ? SliverFillRemaining(
                          child: Center(
                            child: Column(
                              mainAxisAlignment: MainAxisAlignment.center,
                              children: [
                                Icon(
                                  Icons.business,
                                  size: 64,
                                  color: Colors.grey[400],
                                ),
                                const SizedBox(height: 16),
                                Text(
                                  'Нет бирж',
                                  style: Theme.of(context)
                                      .textTheme
                                      .titleLarge
                                      ?.copyWith(color: Colors.grey[600]),
                                ),
                                const SizedBox(height: 8),
                                Text(
                                  'Нажмите + чтобы добавить',
                                  style: TextStyle(color: Colors.grey[600]),
                                ),
                              ],
                            ),
                          ),
                        )
                      : SliverPadding(
                          padding: const EdgeInsets.symmetric(horizontal: 16),
                          sliver: SliverList(
                            delegate: SliverChildBuilderDelegate(
                              (context, index) {
                                return _buildPlatformCard(_platforms[index]);
                              },
                              childCount: _platforms.length,
                            ),
                          ),
                        ),
                  const SliverToBoxAdapter(
                    child: SizedBox(height: 80),
                  ),
                ],
              ),
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _showAddPlatformDialog(context),
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildPlatformCard(CryptoPlatform platform) {
    final isSyncing = _syncing[platform.id] ?? false;

    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: ListTile(
        leading: Container(
          width: 48,
          height: 48,
          decoration: BoxDecoration(
            color: Colors.blue.withOpacity(0.1),
            borderRadius: BorderRadius.circular(8),
          ),
          child: const Icon(Icons.business, color: Colors.blue),
        ),
        title: Text(
          platform.name,
          style: const TextStyle(fontWeight: FontWeight.w600),
        ),
        subtitle: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            if (platform.totalValueUsd != null)
              Text(
                currencyFormat.format(platform.totalValueUsd!),
                style: const TextStyle(
                  fontWeight: FontWeight.bold,
                  fontSize: 14,
                ),
              ),
            Text(
              '${platform.assetsCount} актив(ов)',
              style: TextStyle(
                color: Colors.grey[600],
                fontSize: 12,
              ),
            ),
            if (!platform.isActive)
              Text(
                'Неактивна',
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 12,
                ),
              ),
          ],
        ),
        trailing: isSyncing
            ? const SizedBox(
                width: 20,
                height: 20,
                child: CircularProgressIndicator(strokeWidth: 2),
              )
            : IconButton(
                icon: const Icon(Icons.sync),
                onPressed: () => _syncPlatform(platform.id),
              ),
      ),
    );
  }

  void _showAddPlatformDialog(BuildContext context) {
    showDialog(
      context: context,
      builder: (context) => const _AddPlatformDialog(),
    );
  }
}

class _AddPlatformDialog extends StatefulWidget {
  const _AddPlatformDialog();

  @override
  State<_AddPlatformDialog> createState() => _AddPlatformDialogState();
}

class _AddPlatformDialogState extends State<_AddPlatformDialog> {
  final _formKey = GlobalKey<FormState>();
  final _nameController = TextEditingController();
  final _apiKeyController = TextEditingController();
  final _apiSecretController = TextEditingController();
  final _passphraseController = TextEditingController();

  PopularExchange? _selectedExchange;
  bool _showHelp = false;
  bool _isLoading = false;

  @override
  void dispose() {
    _nameController.dispose();
    _apiKeyController.dispose();
    _apiSecretController.dispose();
    _passphraseController.dispose();
    super.dispose();
  }

  void _selectExchange(PopularExchange exchange) {
    setState(() {
      _selectedExchange = exchange;
      _nameController.text = exchange.name;
    });
  }

  void _clearSelection() {
    setState(() {
      _selectedExchange = null;
      _nameController.clear();
    });
  }

  Future<void> _submit() async {
    if (!_formKey.currentState!.validate()) {
      return;
    }

    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final result = await apiService.createPlatform(
      name: _nameController.text,
      apiKey: _apiKeyController.text.isNotEmpty ? _apiKeyController.text : null,
      apiSecret: _apiSecretController.text.isNotEmpty ? _apiSecretController.text : null,
      passphrase: _passphraseController.text.isNotEmpty ? _passphraseController.text : null,
    );

    if (mounted) {
      setState(() {
        _isLoading = false;
      });

      if (result != null && result['success'] == true) {
        Navigator.of(context).pop();
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Биржа добавлена'),
            backgroundColor: Colors.green,
          ),
        );
      } else {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(result?['message'] ?? 'Ошибка добавления'),
            backgroundColor: Colors.red,
          ),
        );
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Dialog(
      child: Container(
        width: MediaQuery.of(context).size.width * 0.9,
        constraints: BoxConstraints(maxHeight: MediaQuery.of(context).size.height * 0.85),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            // Header
            Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: Theme.of(context).primaryColor,
                borderRadius: const BorderRadius.only(
                  topLeft: Radius.circular(12),
                  topRight: Radius.circular(12),
                ),
              ),
              child: Row(
                children: [
                  const Icon(Icons.add_business, color: Colors.white),
                  const SizedBox(width: 12),
                  const Expanded(
                    child: Text(
                      'Добавить биржу',
                      style: TextStyle(
                        color: Colors.white,
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),
                  IconButton(
                    icon: Icon(_showHelp ? Icons.help_outline : Icons.help),
                    color: Colors.white,
                    onPressed: () {
                      setState(() {
                        _showHelp = !_showHelp;
                      });
                    },
                  ),
                ],
              ),
            ),
            // Content
            Expanded(
              child: SingleChildScrollView(
                padding: const EdgeInsets.all(16),
                child: Form(
                  key: _formKey,
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      // Popular exchanges
                      if (!_showHelp) ...[
                        const Text(
                          'Популярные биржи',
                          style: TextStyle(
                            fontSize: 16,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        const SizedBox(height: 12),
                        SizedBox(
                          height: 100,
                          child: ListView.builder(
                            scrollDirection: Axis.horizontal,
                            itemCount: PopularExchange.all.length,
                            itemBuilder: (context, index) {
                              final exchange = PopularExchange.all[index];
                              final isSelected = _selectedExchange?.name == exchange.name;

                              return Card(
                                elevation: isSelected ? 4 : 1,
                                color: isSelected
                                    ? Theme.of(context).primaryColor.withOpacity(0.1)
                                    : null,
                                child: InkWell(
                                  onTap: () => _selectExchange(exchange),
                                  borderRadius: BorderRadius.circular(8),
                                  child: Padding(
                                    padding: const EdgeInsets.all(12),
                                    child: Column(
                                      mainAxisAlignment: MainAxisAlignment.center,
                                      children: [
                                        Text(
                                          exchange.logoEmoji,
                                          style: const TextStyle(fontSize: 32),
                                        ),
                                        const SizedBox(height: 4),
                                        Text(
                                          exchange.displayName,
                                          style: const TextStyle(fontSize: 12),
                                        ),
                                      ],
                                    ),
                                  ),
                                ),
                              );
                            },
                          ),
                        ),
                        const SizedBox(height: 12),
                        if (_selectedExchange != null)
                          Row(
                            children: [
                              Chip(
                                label: Text(_selectedExchange!.displayName),
                                avatar: Text(_selectedExchange!.logoEmoji),
                                onDeleted: _clearSelection,
                              ),
                              const SizedBox(width: 8),
                              TextButton.icon(
                                onPressed: () {
                                  setState(() {
                                    _showHelp = true;
                                  });
                                },
                                icon: const Icon(Icons.help_outline),
                                label: const Text('Инструкция'),
                              ),
                            ],
                          ),
                        const Divider(height: 24),
                      ],
                      // Help instructions
                      if (_showHelp && _selectedExchange != null) ...[
                        Container(
                          padding: const EdgeInsets.all(12),
                          decoration: BoxDecoration(
                            color: Colors.blue.withOpacity(0.1),
                            borderRadius: BorderRadius.circular(8),
                            border: Border.all(color: Colors.blue.withOpacity(0.3)),
                          ),
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Row(
                                children: [
                                  Text(_selectedExchange!.logoEmoji, style: const TextStyle(fontSize: 24)),
                                  const SizedBox(width: 8),
                                  Expanded(
                                    child: Text(
                                      'Инструкция для ${_selectedExchange!.displayName}',
                                      style: const TextStyle(
                                        fontWeight: FontWeight.bold,
                                        fontSize: 16,
                                      ),
                                    ),
                                  ),
                                  IconButton(
                                    icon: const Icon(Icons.close),
                                    onPressed: () {
                                      setState(() {
                                        _showHelp = false;
                                      });
                                    },
                                  ),
                                ],
                              ),
                              const SizedBox(height: 12),
                              ..._selectedExchange!.keyInstructions.map((instruction) => Padding(
                                padding: const EdgeInsets.only(bottom: 4),
                                child: Text(
                                  instruction,
                                  style: const TextStyle(fontSize: 13),
                                ),
                              )),
                              const SizedBox(height: 12),
                              TextButton.icon(
                                onPressed: () {
                                  // TODO: Open URL in browser
                                },
                                icon: const Icon(Icons.open_in_browser),
                                label: const Text('Открыть на сайте биржи'),
                              ),
                            ],
                          ),
                        ),
                        const SizedBox(height: 16),
                      ],
                      // Form fields
                      TextFormField(
                        controller: _nameController,
                        decoration: const InputDecoration(
                          labelText: 'Название биржи',
                          prefixIcon: Icon(Icons.business),
                          border: OutlineInputBorder(),
                          filled: true,
                        ),
                        validator: (value) {
                          if (value == null || value.isEmpty) {
                            return 'Введите название';
                          }
                          return null;
                        },
                      ),
                      const SizedBox(height: 16),
                      TextFormField(
                        controller: _apiKeyController,
                        decoration: InputDecoration(
                          labelText: 'API Key',
                          prefixIcon: const Icon(Icons.key),
                          border: const OutlineInputBorder(),
                          filled: true,
                          suffixIcon: IconButton(
                            icon: const Icon(Icons.content_copy),
                            onPressed: () {
                              Clipboard.setData(ClipboardData(text: _apiKeyController.text));
                              ScaffoldMessenger.of(context).showSnackBar(
                                const SnackBar(content: Text('API Key скопирован')),
                              );
                            },
                          ),
                        ),
                        validator: (value) {
                          if (value == null || value.isEmpty) {
                            return 'Введите API Key';
                          }
                          return null;
                        },
                      ),
                      const SizedBox(height: 16),
                      TextFormField(
                        controller: _apiSecretController,
                        decoration: InputDecoration(
                          labelText: 'API Secret',
                          prefixIcon: const Icon(Icons.lock),
                          border: const OutlineInputBorder(),
                          filled: true,
                          suffixIcon: Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              IconButton(
                                icon: const Icon(Icons.visibility),
                                onPressed: () {
                                  // TODO: Toggle visibility
                                },
                              ),
                              IconButton(
                                icon: const Icon(Icons.content_copy),
                                onPressed: () {
                                  Clipboard.setData(ClipboardData(text: _apiSecretController.text));
                                  ScaffoldMessenger.of(context).showSnackBar(
                                    const SnackBar(content: Text('Secret скопирован')),
                                  );
                                },
                              ),
                            ],
                          ),
                        ),
                        obscureText: true,
                        validator: (value) {
                          if (value == null || value.isEmpty) {
                            return 'Введите API Secret';
                          }
                          return null;
                        },
                      ),
                      const SizedBox(height: 16),
                      TextFormField(
                        controller: _passphraseController,
                        decoration: InputDecoration(
                          labelText: 'Passphrase (если требуется)',
                          prefixIcon: const Icon(Icons.password),
                          border: const OutlineInputBorder(),
                          filled: true,
                          helperText: 'Требуется для KuCoin, OKX и др.',
                          suffixIcon: _passphraseController.text.isNotEmpty
                              ? IconButton(
                                  icon: const Icon(Icons.content_copy),
                                  onPressed: () {
                                    Clipboard.setData(ClipboardData(text: _passphraseController.text));
                                    ScaffoldMessenger.of(context).showSnackBar(
                                      const SnackBar(content: Text('Passphrase скопирован')),
                                    );
                                  },
                                )
                              : null,
                        ),
                        obscureText: true,
                      ),
                      const SizedBox(height: 8),
                      Text(
                        '⚠️ API ключи будут использоваться только для чтения данных',
                        style: TextStyle(
                          fontSize: 12,
                          color: Colors.orange[700],
                        ),
                      ),
                    ],
                  ),
                ),
              ),
            ),
            // Actions
            Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: Colors.grey[100],
                borderRadius: const BorderRadius.only(
                  bottomLeft: Radius.circular(12),
                  bottomRight: Radius.circular(12),
                ),
              ),
              child: Row(
                children: [
                  Expanded(
                    child: OutlinedButton(
                      onPressed: _isLoading ? null : () => Navigator.of(context).pop(),
                      child: const Text('Отмена'),
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: ElevatedButton(
                      onPressed: _isLoading ? null : _submit,
                      child: _isLoading
                          ? const SizedBox(
                              width: 20,
                              height: 20,
                              child: CircularProgressIndicator(strokeWidth: 2),
                            )
                          : const Text('Добавить'),
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
