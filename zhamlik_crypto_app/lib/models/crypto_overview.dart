import 'crypto_asset.dart';

class CryptoOverview {
  final double totalValueUsd;
  final double totalValueRub;
  final double totalValueBtc;
  final double change24h;
  final double change7d;
  final double change30d;
  final int assetsCount;
  final int platformsCount;
  final List<AssetDistribution> distribution;
  final List<CryptoAsset> topAssets;

  CryptoOverview({
    required this.totalValueUsd,
    required this.totalValueRub,
    required this.totalValueBtc,
    required this.change24h,
    required this.change7d,
    required this.change30d,
    required this.assetsCount,
    required this.platformsCount,
    required this.distribution,
    required this.topAssets,
  });

  factory CryptoOverview.fromJson(Map<String, dynamic> json) {
    final assetsList = (json['assets'] as List?)
        ?.map((e) => CryptoAsset.fromJson(e as Map<String, dynamic>))
        .toList() ?? [];

    final assetsCount = assetsList.length;

    final distributionList = <AssetDistribution>[];
    final platformsSet = <String>{};

    for (var asset in assetsList) {
      platformsSet.add(asset.ticker);

      final existingIndex = distributionList.indexWhere((d) => d.ticker == asset.ticker);
      if (existingIndex >= 0) {
        final existing = distributionList[existingIndex];
        distributionList[existingIndex] = AssetDistribution(
          ticker: asset.ticker,
          name: asset.name,
          valueUsd: existing.valueUsd + asset.valueUsd,
          percentage: existing.percentage,
        );
      } else {
        distributionList.add(AssetDistribution(
          ticker: asset.ticker,
          name: asset.name,
          valueUsd: asset.valueUsd,
          percentage: 0.0,
        ));
      }
    }

    final totalValue = (json['total_balance_usd'] as num?)?.toDouble() ?? 0.0;
    for (var i = 0; i < distributionList.length; i++) {
      final d = distributionList[i];
      distributionList[i] = AssetDistribution(
        ticker: d.ticker,
        name: d.name,
        valueUsd: d.valueUsd,
        percentage: totalValue > 0 ? (d.valueUsd / totalValue * 100) : 0.0,
      );
    }

    return CryptoOverview(
      totalValueUsd: totalValue,
      totalValueRub: (json['total_balance_rub'] as num?)?.toDouble() ?? 0.0,
      totalValueBtc: totalValue > 0 ? totalValue / _getBtcPrice() : 0.0,
      change24h: (json['change_24h'] as num?)?.toDouble() ?? 0.0,
      change7d: (json['change_7d'] as num?)?.toDouble() ?? 0.0,
      change30d: (json['change_30d'] as num?)?.toDouble() ?? 0.0,
      assetsCount: assetsCount,
      platformsCount: platformsSet.length,
      distribution: distributionList,
      topAssets: assetsList,
    );
  }

  static double _getBtcPrice() {
    return 95000.0;
  }

  Map<String, dynamic> toJson() {
    return {
      'total_value_usd': totalValueUsd,
      'total_value_rub': totalValueRub,
      'total_value_btc': totalValueBtc,
      'change_24h': change24h,
      'change_7d': change7d,
      'change_30d': change30d,
      'assets_count': assetsCount,
      'platforms_count': platformsCount,
      'distribution': distribution.map((e) => e.toJson()).toList(),
      'top_assets': topAssets.map((e) => e.toJson()).toList(),
    };
  }

  String get change24hPercent {
    final sign = change24h >= 0 ? '+' : '';
    return '$sign${change24h.toStringAsFixed(2)}%';
  }

  bool get isPositive24h => change24h >= 0;
}

class AssetDistribution {
  final String ticker;
  final String name;
  final double valueUsd;
  final double percentage;

  AssetDistribution({
    required this.ticker,
    required this.name,
    required this.valueUsd,
    required this.percentage,
  });

  factory AssetDistribution.fromJson(Map<String, dynamic> json) {
    return AssetDistribution(
      ticker: json['ticker'] as String,
      name: json['name'] as String,
      valueUsd: (json['value_usd'] as num).toDouble(),
      percentage: (json['percentage'] as num).toDouble(),
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'ticker': ticker,
      'name': name,
      'value_usd': valueUsd,
      'percentage': percentage,
    };
  }
}
