class CryptoPlatform {
  final int id;
  final String name;
  final bool isActive;
  final bool hasApiKeys;
  final int assetsCount;
  final double? totalValueUsd;
  final String? notes;
  final String? lastSyncStatus;
  final DateTime? lastSyncedAt;

  CryptoPlatform({
    required this.id,
    required this.name,
    required this.isActive,
    required this.hasApiKeys,
    required this.assetsCount,
    this.totalValueUsd,
    this.notes,
    this.lastSyncStatus,
    this.lastSyncedAt,
  });

  factory CryptoPlatform.fromJson(Map<String, dynamic> json) {
    return CryptoPlatform(
      id: json['id'] as int,
      name: json['name'] as String,
      isActive: json['is_active'] as bool? ?? json['is_active'] == true,
      hasApiKeys: json['has_api_keys'] as bool? ?? false,
      assetsCount: json['assets_count'] as int? ?? 0,
      totalValueUsd: json['total_value_usd'] != null
          ? (json['total_value_usd'] as num).toDouble()
          : null,
      notes: json['notes'] as String?,
      lastSyncStatus: json['last_sync_status'] as String?,
      lastSyncedAt: json['last_synced_at'] != null
          ? DateTime.parse(json['last_synced_at'])
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'name': name,
      'is_active': isActive,
      'has_api_keys': hasApiKeys,
      'assets_count': assetsCount,
      'total_value_usd': totalValueUsd,
      'notes': notes,
      'last_sync_status': lastSyncStatus,
      'last_synced_at': lastSyncedAt?.toIso8601String(),
    };
  }
}
