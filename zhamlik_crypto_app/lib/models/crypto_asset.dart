class CryptoAsset {
  final int? id; // Опционально, т.к. API overview не возвращает id
  final String ticker;
  final String name;
  final double quantity;
  final double priceUsd; // Опционально для API overview
  final double valueUsd;
  final double? change24h;
  final String? sourceAccountType;
  final int? platformId;
  final String? platformName;

  CryptoAsset({
    this.id,
    required this.ticker,
    required this.name,
    required this.quantity,
    this.priceUsd = 0.0,
    required this.valueUsd,
    this.change24h,
    this.sourceAccountType,
    this.platformId,
    this.platformName,
  });

  factory CryptoAsset.fromJson(Map<String, dynamic> json) {
    return CryptoAsset(
      id: json['id'] as int?,
      ticker: json['ticker'] as String? ?? '',
      name: json['name'] as String? ?? json['ticker'] ?? '',
      quantity: (json['quantity'] as num?)?.toDouble() ?? 0.0,
      priceUsd: (json['price_usd'] as num?)?.toDouble() ?? 0.0,
      valueUsd: (json['value_usd'] as num?)?.toDouble() ?? 0.0,
      change24h: json['change_24h'] != null
          ? (json['change_24h'] as num).toDouble()
          : null,
      sourceAccountType: json['source_account_type'] as String?,
      platformId: json['platform_id'] as int?,
      platformName: json['platform_name'] as String?,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'ticker': ticker,
      'name': name,
      'quantity': quantity,
      'price_usd': priceUsd,
      'value_usd': valueUsd,
      'change_24h': change24h,
      'source_account_type': sourceAccountType,
      'platform_id': platformId,
      'platform_name': platformName,
    };
  }

  String get changePercent {
    if (change24h == null) return '0.00%';
    final sign = change24h! >= 0 ? '+' : '';
    return '$sign${change24h!.toStringAsFixed(2)}%';
  }

  bool get isPositive => change24h != null && change24h! >= 0;
}
