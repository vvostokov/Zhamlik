import 'package:flutter/material.dart';
import 'package:animations/animations.dart';

class CryptoAssetCard extends StatelessWidget {
  final String ticker;
  final String name;
  final double quantity;
  final double valueUsd;
  final double priceUsd;
  final double? change24h;
  final VoidCallback? onTap;

  const CryptoAssetCard({
    super.key,
    required this.ticker,
    required this.name,
    required this.quantity,
    required this.valueUsd,
    required this.priceUsd,
    this.change24h,
    this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    final isPositive = change24h != null && change24h! >= 0;
    final color = isPositive ? Colors.green : Colors.red;

    final currencyFormat = NumberFormat.currency(symbol: '\$', decimalDigits: 2);

    return OpenContainer(
      closedElevation: 2,
      closedShape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(16),
      ),
      closedColor: Colors.white,
      openColor: Colors.white,
      openElevation: 8,
      openShape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(16),
      ),
      transitionDuration: const Duration(milliseconds: 400),
      openBuilder: (context, action, closedWidget) {
        return Scaffold(
          appBar: AppBar(
            title: Text(name),
            backgroundColor: isPositive ? Colors.green : Colors.red,
          ),
          body: Center(
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                Icon(
                  _getCryptoIcon(ticker),
                  size: 80,
                  color: isPositive ? Colors.green : Colors.red,
                ),
                const SizedBox(height: 16),
                Text(
                  ticker.toUpperCase(),
                  style: const TextStyle(
                    fontSize: 32,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                const SizedBox(height: 8),
                Text(
                  currencyFormat.format(valueUsd),
                  style: const TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ],
            ),
          ),
        );
      },
      tappable: onTap != null,
      onTap: onTap,
      child: Container(
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(16),
          border: Border.all(color: Colors.grey[200]!),
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.05),
              blurRadius: 10,
              offset: const Offset(0, 2),
            ),
          ],
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Container(
                  padding: const EdgeInsets.all(12),
                  decoration: BoxDecoration(
                    color: Colors.orange.withOpacity(0.1),
                    shape: BoxShape.circle,
                  ),
                  child: Text(
                    ticker.substring(0, 2).toUpperCase(),
                    style: const TextStyle(
                      fontWeight: FontWeight.bold,
                      color: Colors.orange,
                    ),
                  ),
                ),
                if (change24h != null)
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                    decoration: BoxDecoration(
                      color: color.withOpacity(0.1),
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Text(
                      '${isPositive ? '+' : ''}${change24h!.toStringAsFixed(2)}%',
                      style: TextStyle(
                        color: color,
                        fontWeight: FontWeight.bold,
                        fontSize: 12,
                      ),
                    ),
                  ),
              ],
            ),
            const SizedBox(height: 12),
            Text(
              name,
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 4),
            Text(
              '${quantity.toStringAsFixed(6)} ${ticker.toUpperCase()}',
              style: TextStyle(
                color: Colors.grey[600],
                fontSize: 13,
              ),
            ),
            const SizedBox(height: 12),
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Стоимость',
                      style: TextStyle(
                        color: Colors.grey[600],
                        fontSize: 12,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      currencyFormat.format(valueUsd),
                      style: const TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ],
                ),
                Column(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    Text(
                      'Цена',
                      style: TextStyle(
                        color: Colors.grey[600],
                        fontSize: 12,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      currencyFormat.format(priceUsd),
                      style: const TextStyle(
                        fontSize: 14,
                      ),
                    ),
                  ],
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  IconData _getCryptoIcon(String ticker) {
    switch (ticker.toLowerCase()) {
      case 'btc':
        return Icons.currency_bitcoin;
      case 'eth':
        return Icons.currency_exchange;
      default:
        return Icons.account_balance_wallet;
    }
  }
}
