import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/crypto_api_service.dart';
import '../services/version_service.dart';
import '../models/crypto_overview.dart';
import 'platforms_screen.dart';
import 'assets_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '\$',
    decimalDigits: 2,
  );
  final NumberFormat rubFormat = NumberFormat.currency(
    symbol: '₽',
    decimalDigits: 2,
  );

  CryptoOverview? _overview;
  int _platformsCount = 0;
  bool _isLoading = true;
  bool _hasError = false;
  String? _errorMessage;

  @override
  void initState() {
    super.initState();
    _loadData();
    _checkForUpdates();
  }

  Future<void> _loadData() async {
    setState(() {
      _isLoading = true;
      _hasError = false;
      _errorMessage = null;
    });

    try {
      final apiService = Provider.of<CryptoApiService>(context, listen: false);

      // Load overview and platforms in parallel
      final results = await Future.wait([
        apiService.getOverview(),
        apiService.getPlatforms(),
      ]);

      final overview = results[0] as CryptoOverview?;
      final platforms = results[1] as List;

      if (mounted) {
        setState(() {
          _overview = overview;
          _platformsCount = platforms.length;
          _isLoading = false;
          if (overview == null) {
            _hasError = true;
            _errorMessage = 'Не удалось загрузить данные';
          }
        });
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _isLoading = false;
          _hasError = true;
          _errorMessage = 'Ошибка: ${e.toString()}';
          _overview = null;
        });
      }
    }
  }

  Future<void> _checkForUpdates() async {
    final versionService = Provider.of<VersionService>(context, listen: false);
    await versionService.checkForUpdates();

    if (mounted && versionService.hasUpdate) {
      await VersionService.showUpdateDialogIfNeeded(context, versionService);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Zhamlik Crypto'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: _loadData,
          ),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : _hasError
              ? Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Icon(
                        Icons.error_outline,
                        size: 64,
                        color: Theme.of(context).colorScheme.error,
                      ),
                      const SizedBox(height: 16),
                      Text(
                        _errorMessage ?? 'Ошибка загрузки данных',
                        style: Theme.of(context).textTheme.titleLarge,
                        textAlign: TextAlign.center,
                      ),
                      const SizedBox(height: 16),
                      ElevatedButton.icon(
                        onPressed: _loadData,
                        icon: const Icon(Icons.refresh),
                        label: const Text('Повторить'),
                      ),
                    ],
                  ),
                )
              : RefreshIndicator(
                  onRefresh: _loadData,
                  child: SingleChildScrollView(
                    physics: const AlwaysScrollableScrollPhysics(),
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        _buildPortfolioValueCard(context),
                        const SizedBox(height: 24),
                        _buildChangesCard(context),
                        const SizedBox(height: 24),
                        _buildStatsCard(context),
                        const SizedBox(height: 24),
                        _buildQuickActions(context),
                        const SizedBox(height: 24),
                        if (_overview != null && _overview!.topAssets.isNotEmpty)
                          _buildTopAssets(context),
                      ],
                    ),
                  ),
                ),
    );
  }

  Widget _buildPortfolioValueCard(BuildContext context) {
    final totalUsd = _overview?.totalValueUsd ?? 0.0;
    final totalRub = _overview?.totalValueRub ?? 0.0;
    final totalBtc = _overview?.totalValueBtc ?? 0.0;

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Стоимость портфеля',
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    color: Colors.grey[600],
                  ),
            ),
            const SizedBox(height: 12),
            Text(
              currencyFormat.format(totalUsd),
              style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                    color: Theme.of(context).colorScheme.primary,
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 8),
            Text(
              '≈ ${rubFormat.format(totalRub)}',
              style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    color: Colors.grey[600],
                  ),
            ),
            const SizedBox(height: 4),
            Text(
              '₿ ${totalBtc.toStringAsFixed(8)} BTC',
              style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                    color: Colors.grey[600],
                  ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildChangesCard(BuildContext context) {
    final change24h = _overview?.change24h ?? 0.0;
    final change7d = _overview?.change7d ?? 0.0;
    final change30d = _overview?.change30d ?? 0.0;

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Изменение',
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 16),
            Row(
              children: [
                Expanded(
                  child: _buildChangeItem(context, '24ч', change24h),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: _buildChangeItem(context, '7д', change7d),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: _buildChangeItem(context, '30д', change30d),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildChangeItem(BuildContext context, String period, double change) {
    final isPositive = change >= 0;
    final color = isPositive ? Colors.green : Colors.red;

    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: color.withOpacity(0.1),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Column(
        children: [
          Text(
            period,
            style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: Colors.grey[600],
                ),
          ),
          const SizedBox(height: 4),
          Text(
            '${isPositive ? '+' : ''}${change.toStringAsFixed(2)}%',
            style: Theme.of(context).textTheme.titleLarge?.copyWith(
                  color: color,
                  fontWeight: FontWeight.bold,
                ),
          ),
        ],
      ),
    );
  }

  Widget _buildStatsCard(BuildContext context) {
    final assetsCount = _overview?.assetsCount ?? 0;
    final platformsCount = _platformsCount;

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Row(
          children: [
            Expanded(
              child: _buildStatItem(
                context,
                'Активов',
                assetsCount.toString(),
                Icons.account_balance_wallet,
              ),
            ),
            const SizedBox(width: 16),
            Expanded(
              child: _buildStatItem(
                context,
                'Бирж',
                platformsCount.toString(),
                Icons.business,
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildStatItem(
    BuildContext context,
    String label,
    String value,
    IconData icon,
  ) {
    return Column(
      children: [
        Icon(icon, size: 32, color: Theme.of(context).colorScheme.primary),
        const SizedBox(height: 8),
        Text(
          value,
          style: Theme.of(context).textTheme.headlineSmall?.copyWith(
                fontWeight: FontWeight.bold,
              ),
        ),
        Text(
          label,
          style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                color: Colors.grey[600],
              ),
        ),
      ],
    );
  }

  Widget _buildQuickActions(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Быстрые действия',
          style: Theme.of(context).textTheme.titleLarge?.copyWith(
                fontWeight: FontWeight.bold,
              ),
        ),
        const SizedBox(height: 16),
        Row(
          children: [
            Expanded(
              child: _buildActionButton(
                context,
                'Биржи',
                Icons.business,
                () {
                  Navigator.of(context).pushNamed('/platforms');
                },
                Colors.blue,
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _buildActionButton(
                context,
                'Активы',
                Icons.account_balance_wallet,
                () {
                  Navigator.of(context).pushNamed('/assets');
                },
                Colors.green,
              ),
            ),
          ],
        ),
        const SizedBox(height: 12),
        Row(
          children: [
            Expanded(
              child: _buildActionButton(
                context,
                'Фьючерсы',
                Icons.trending_up,
                () {
                  Navigator.of(context).pushNamed('/futures');
                },
                Colors.teal,
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _buildActionButton(
                context,
                'История',
                Icons.receipt_long,
                () {
                  Navigator.of(context).pushNamed('/transactions');
                },
                Colors.purple,
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildActionButton(
    BuildContext context,
    String label,
    IconData icon,
    VoidCallback onTap,
    Color color,
  ) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(12),
      child: Container(
        padding: const EdgeInsets.symmetric(vertical: 20),
        decoration: BoxDecoration(
          color: color.withOpacity(0.1),
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: color.withOpacity(0.3)),
        ),
        child: Column(
          children: [
            Icon(icon, color: color, size: 32),
            const SizedBox(height: 8),
            Text(
              label,
              style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                    color: color,
                    fontWeight: FontWeight.w600,
                  ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildTopAssets(BuildContext context) {
    final topAssets = _overview!.topAssets.take(5).toList();

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Text(
              'Топ активы',
              style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            TextButton(
              onPressed: () {
                Navigator.of(context).pushNamed('/assets');
              },
              child: const Text('Все'),
            ),
          ],
        ),
        const SizedBox(height: 12),
        Card(
          child: Column(
            children: topAssets.map((asset) {
              return ListTile(
                leading: CircleAvatar(
                  child: Text(asset.ticker.substring(0, 2).toUpperCase()),
                ),
                title: Text(
                  asset.name,
                  style: const TextStyle(fontWeight: FontWeight.w500),
                ),
                subtitle: Text(
                  '${asset.quantity.toStringAsFixed(4)} ${asset.ticker.toUpperCase()}',
                ),
                trailing: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    Text(
                      currencyFormat.format(asset.valueUsd),
                      style: const TextStyle(fontWeight: FontWeight.bold),
                    ),
                    Text(
                      asset.changePercent,
                      style: TextStyle(
                        color: asset.isPositive ? Colors.green : Colors.red,
                        fontSize: 12,
                      ),
                    ),
                  ],
                ),
              );
            }).toList(),
          ),
        ),
      ],
    );
  }
}
