import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/crypto_api_service.dart';
import '../models/crypto_asset.dart';
import '../models/crypto_platform.dart';

class AssetsScreen extends StatefulWidget {
  const AssetsScreen({super.key});

  @override
  State<AssetsScreen> createState() => _AssetsScreenState();
}

class _AssetsScreenState extends State<AssetsScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '\$',
    decimalDigits: 2,
  );

  Map<int, List<CryptoAsset>> _assetsByPlatform = {};
  List<CryptoPlatform> _platforms = [];
  bool _isLoading = true;
  String _sortBy = 'value'; // 'value', 'name', 'change'

  @override
  void initState() {
    super.initState();
    _loadData();
  }

  Future<void> _loadData() async {
    setState(() {
      _isLoading = true;
    });

    // Load platforms first, then assets
    await _loadPlatforms();
    await _loadAssets();
  }

  Future<void> _loadPlatforms() async {
    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final platforms = await apiService.getPlatforms();

    if (mounted) {
      setState(() {
        _platforms = platforms;
      });
    }
  }

  Future<void> _loadAssets() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final Map<int, List<CryptoAsset>> grouped = {};

    // Load assets for each platform separately
    for (var platform in _platforms) {
      if (platform.isActive) {
        final platformAssets = await apiService.getAssets(platformId: platform.id);
        // Add platform_id to each asset
        final assetsWithPlatform = platformAssets.map((asset) {
          return CryptoAsset(
            id: asset.id,
            ticker: asset.ticker,
            name: asset.name,
            quantity: asset.quantity,
            priceUsd: asset.priceUsd,
            valueUsd: asset.valueUsd,
            change24h: asset.change24h,
            sourceAccountType: asset.sourceAccountType,
            platformId: platform.id,
            platformName: platform.name,
          );
        }).toList();

        if (assetsWithPlatform.isNotEmpty) {
          grouped[platform.id] = assetsWithPlatform;
        }
      }
    }

    if (mounted) {
      setState(() {
        _assetsByPlatform = grouped;
        _isLoading = false;
      });
    }
  }

  Map<int, List<CryptoAsset>> _groupAssetsByPlatform(List<CryptoAsset> assets) {
    final grouped = <int, List<CryptoAsset>>{};

    for (var asset in assets) {
      if (asset.platformId != null) {
        grouped.putIfAbsent(asset.platformId!, () => []);
        grouped[asset.platformId]!.add(asset);
      }
    }

    return grouped;
  }

  double _getTotalValue() {
    double total = 0;
    for (var assets in _assetsByPlatform.values) {
      for (var asset in assets) {
        total += asset.valueUsd;
      }
    }
    return total;
  }

  void _sortAssets() {
    setState(() {
      // Сортируем активы внутри каждой биржи
      _assetsByPlatform.forEach((platformId, assets) {
        switch (_sortBy) {
          case 'value':
            assets.sort((a, b) => b.valueUsd.compareTo(a.valueUsd));
            break;
          case 'name':
            assets.sort((a, b) => a.name.compareTo(b.name));
            break;
          case 'change':
            assets.sort((a, b) =>
                ((b.change24h ?? 0).compareTo(a.change24h ?? 0)));
            break;
        }
      });
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Крипто-активы'),
        actions: [
          PopupMenuButton<String>(
            onSelected: (value) {
              if (value != null) {
                setState(() {
                  _sortBy = value;
                });
                _sortAssets();
              }
            },
            itemBuilder: (context) => [
              const PopupMenuItem(
                value: 'value',
                child: Text('Сортировать по стоимости'),
              ),
              const PopupMenuItem(
                value: 'name',
                child: Text('Сортировать по названию'),
              ),
              const PopupMenuItem(
                value: 'change',
                child: Text('Сортировать по изменению'),
              ),
            ],
          ),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
              onRefresh: _loadData,
              child: Column(
                children: [
                  _buildTotalValueCard(context),
                  const SizedBox(height: 16),
                  Expanded(
                    child: _assetsByPlatform.isEmpty
                        ? _buildEmptyState()
                        : _buildPlatformsList(),
                  ),
                ],
              ),
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _showAddAssetDialog(context),
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildTotalValueCard(BuildContext context) {
    final totalValue = _getTotalValue();

    return Card(
      margin: const EdgeInsets.all(16),
      child: Padding(
        padding: const EdgeInsets.all(20),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  'Общая стоимость портфеля',
                  style: Theme.of(context).textTheme.titleMedium?.copyWith(
                        color: Colors.grey[600],
                      ),
                ),
                const SizedBox(height: 8),
                Text(
                  currencyFormat.format(totalValue),
                  style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                        color: Theme.of(context).colorScheme.primary,
                        fontWeight: FontWeight.bold,
                      ),
                ),
              ],
            ),
            Icon(
              Icons.account_balance_wallet,
              size: 48,
              color: Theme.of(context).colorScheme.primary,
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(
            Icons.account_balance_wallet,
            size: 80,
            color: Colors.grey[400],
          ),
          const SizedBox(height: 16),
          Text(
            'Нет активов',
            style: Theme.of(context).textTheme.titleLarge?.copyWith(
                  color: Colors.grey[600],
                ),
          ),
          const SizedBox(height: 8),
          Text(
            'Добавьте биржу для синхронизации',
            style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                  color: Colors.grey[500],
                ),
          ),
        ],
      ),
    );
  }

  Widget _buildPlatformsList() {
    return ListView.builder(
      itemCount: _assetsByPlatform.keys.length,
      itemBuilder: (context, index) {
        final platformId = _assetsByPlatform.keys.elementAt(index);
        final assets = _assetsByPlatform[platformId]!;
        final platform = _platforms.firstWhere(
          (p) => p.id == platformId,
          orElse: () => CryptoPlatform(
            id: platformId,
            name: 'Биржа $platformId',
            isActive: true,
            hasApiKeys: false,
            assetsCount: assets.length,
          ),
        );

        return _buildPlatformCard(platform, assets);
      },
    );
  }

  Widget _buildPlatformCard(CryptoPlatform platform, List<CryptoAsset> assets) {
    final platformTotal = assets.fold<double>(
      0.0,
      (sum, asset) => sum + asset.valueUsd,
    );

    return Card(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Заголовок биржи с общей стоимостью
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: Theme.of(context).colorScheme.primary.withOpacity(0.1),
              borderRadius: const BorderRadius.only(
                topLeft: Radius.circular(16),
                topRight: Radius.circular(16),
              ),
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Row(
                      children: [
                        Icon(
                          Icons.business,
                          color: Theme.of(context).colorScheme.primary,
                          size: 24,
                        ),
                        const SizedBox(width: 12),
                        Text(
                          platform.name,
                          style: Theme.of(context).textTheme.titleLarge?.copyWith(
                                fontWeight: FontWeight.bold,
                                color: Theme.of(context).colorScheme.primary,
                              ),
                        ),
                      ],
                    ),
                    Text(
                      '${assets.length} актив(ов)',
                      style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                            color: Colors.grey[600],
                          ),
                    ),
                  ],
                ),
                const SizedBox(height: 8),
                Text(
                  currencyFormat.format(platformTotal),
                  style: Theme.of(context).textTheme.headlineSmall?.copyWith(
                        color: Theme.of(context).colorScheme.primary,
                        fontWeight: FontWeight.bold,
                      ),
                ),
              ],
            ),
          ),
          // Список активов на этой бирже
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 0, 16, 16),
            child: Column(
              children: assets.map((asset) {
                return _buildAssetTile(asset);
              }).toList(),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildAssetTile(CryptoAsset asset) {
    return ListTile(
      contentPadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      leading: CircleAvatar(
        backgroundColor: Colors.orange.withOpacity(0.1),
        child: Text(
          asset.ticker.substring(0, 2).toUpperCase(),
          style: const TextStyle(
            fontWeight: FontWeight.bold,
            color: Colors.orange,
          ),
        ),
      ),
      title: Text(
        asset.name,
        style: const TextStyle(fontWeight: FontWeight.w600),
      ),
      subtitle: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            '${asset.quantity.toStringAsFixed(6)} ${asset.ticker.toUpperCase()}',
            style: const TextStyle(fontSize: 13),
          ),
          Text(
            '${currencyFormat.format(asset.priceUsd)} за единицу',
            style: TextStyle(
              fontSize: 12,
              color: Colors.grey[600],
            ),
          ),
        ],
      ),
      trailing: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          Text(
            currencyFormat.format(asset.valueUsd),
            style: const TextStyle(
              fontWeight: FontWeight.bold,
              fontSize: 16,
            ),
          ),
          if (asset.change24h != null)
            Container(
              margin: const EdgeInsets.only(top: 4),
              padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
              decoration: BoxDecoration(
                color: asset.isPositive
                    ? Colors.green.withOpacity(0.1)
                    : Colors.red.withOpacity(0.1),
                borderRadius: BorderRadius.circular(4),
              ),
              child: Text(
                asset.changePercent,
                style: TextStyle(
                  color: asset.isPositive ? Colors.green : Colors.red,
                  fontSize: 11,
                  fontWeight: FontWeight.bold,
                ),
              ),
            ),
        ],
      ),
    );
  }

  void _showAddAssetDialog(BuildContext context) {
    final tickerController = TextEditingController();
    final quantityController = TextEditingController();
    CryptoPlatform? _selectedPlatform = _platforms.isNotEmpty ? _platforms.first : null;

    showDialog(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) => AlertDialog(
          title: const Text('Добавить актив'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                DropdownButtonFormField<CryptoPlatform>(
                  value: _selectedPlatform,
                  decoration: const InputDecoration(
                    labelText: 'Биржа',
                    prefixIcon: Icon(Icons.business),
                    border: OutlineInputBorder(),
                  ),
                  items: _platforms.map((platform) {
                    return DropdownMenuItem(
                      value: platform,
                      child: Row(
                        children: [
                          const Icon(Icons.business, size: 20),
                          const SizedBox(width: 12),
                          Expanded(
                            child: Text(
                              platform.name,
                              style: const TextStyle(fontSize: 14),
                            ),
                          ),
                          Text(
                            '${platform.assetsCount} активов',
                            style: TextStyle(
                              fontSize: 12,
                              color: Colors.grey[600],
                            ),
                          ),
                        ],
                      ),
                    );
                  }).toList(),
                  onChanged: (platform) {
                    setDialogState(() {
                      _selectedPlatform = platform;
                    });
                  },
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: tickerController,
                  decoration: const InputDecoration(
                    labelText: 'Тикер (например, BTC)',
                    prefixIcon: Icon(Icons.currency_bitcoin),
                    border: OutlineInputBorder(),
                  ),
                  textCapitalization: TextCapitalization.characters,
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: quantityController,
                  decoration: const InputDecoration(
                    labelText: 'Количество',
                    prefixIcon: Icon(Icons.pin),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: const TextInputType.numberWithOptions(decimal: true),
                ),
                const SizedBox(height: 8),
                Text(
                  '⚠️ Автоматически добавится на выбранную биржу',
                  style: TextStyle(
                    fontSize: 12,
                    color: Colors.orange[700],
                  ),
                ),
              ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Отмена'),
            ),
            ElevatedButton(
              onPressed: () async {
                final ticker = tickerController.text.trim();
                final quantity = double.tryParse(quantityController.text);

                if (ticker.isEmpty) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Введите тикер')),
                  );
                  return;
                }

                if (quantity == null || quantity <= 0) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Введите корректное количество')),
                  );
                  return;
                }

                if (_selectedPlatform == null) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Выберите биржу')),
                  );
                  return;
                }

                Navigator.of(context).pop();

                final apiService = Provider.of<CryptoApiService>(context, listen: false);
                final result = await apiService.createAsset(
                  platformId: _selectedPlatform!.id,
                  ticker: ticker,
                  quantity: quantity,
                );

                if (result != null && result['success'] == true) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Актив добавлен'),
                      backgroundColor: Colors.green,
                    ),
                  );
                  _loadData();
                } else {
                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(
                      content: Text(result?['message'] ?? 'Ошибка при добавлении актива'),
                      backgroundColor: Colors.red,
                    ),
                  );
                }
              },
              child: const Text('Добавить'),
            ),
          ],
        ),
      ),
    );
  }
}
