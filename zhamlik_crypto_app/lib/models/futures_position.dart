class FuturesPosition {
  final int platformId;
  final String platformName;
  final String symbol;
  final String side;
  final double size;
  final double entryPrice;
  final double markPrice;
  final double unrealizedPnl;
  final double fees;
  final int leverage;

  FuturesPosition({
    required this.platformId,
    required this.platformName,
    required this.symbol,
    required this.side,
    required this.size,
    required this.entryPrice,
    required this.markPrice,
    required this.unrealizedPnl,
    required this.fees,
    required this.leverage,
  });

  factory FuturesPosition.fromJson(Map<String, dynamic> json) {
    return FuturesPosition(
      platformId: json['platform_id'] as int,
      platformName: json['platform_name'] as String,
      symbol: json['symbol'] as String,
      side: json['side'] as String,
      size: (json['size'] as num).toDouble(),
      entryPrice: (json['entry_price'] as num).toDouble(),
      markPrice: (json['mark_price'] as num).toDouble(),
      unrealizedPnl: (json['unrealized_pnl'] as num?)?.toDouble() ?? 0.0,
      fees: (json['fees'] as num?)?.toDouble() ?? 0.0,
      leverage: (json['leverage'] as num?)?.toInt() ?? 1,
    );
  }

  double get pnlPercent {
    if (entryPrice <= 0 || size <= 0) return 0;
    return (unrealizedPnl / (entryPrice * size)) * 100;
  }

  bool get isLong => side.toLowerCase() == 'long' || side.toLowerCase() == 'buy';
  bool get isProfit => unrealizedPnl >= 0;

  double get positionValue => entryPrice * size;
}

class FuturesOverview {
  final double totalUnrealizedPnl;
  final int totalPositions;
  final int platformsWithPositions;
  final int totalPlatforms;

  FuturesOverview({
    required this.totalUnrealizedPnl,
    required this.totalPositions,
    required this.platformsWithPositions,
    required this.totalPlatforms,
  });

  factory FuturesOverview.fromJson(Map<String, dynamic> json) {
    return FuturesOverview(
      totalUnrealizedPnl: (json['total_unrealized_pnl'] as num?)?.toDouble() ?? 0.0,
      totalPositions: json['total_positions'] as int? ?? 0,
      platformsWithPositions: json['platforms_with_positions'] as int? ?? 0,
      totalPlatforms: json['total_platforms'] as int? ?? 0,
    );
  }

  String get pnlFormatted {
    final sign = totalUnrealizedPnl >= 0 ? '+' : '';
    return '$sign\$${totalUnrealizedPnl.toStringAsFixed(2)}';
  }

  bool get isProfit => totalUnrealizedPnl >= 0;
}
