import 'package:flutter/material.dart';
import 'package:animations/animations.dart';

class AnimatedStatCard extends StatelessWidget {
  final String title;
  final double value;
  final String subtitle;
  final IconData icon;
  final Color color;
  final VoidCallback? onTap;

  const AnimatedStatCard({
    super.key,
    required this.title,
    required this.value,
    required this.subtitle,
    required this.icon,
    required this.color,
    this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    final NumberFormat currencyFormat = NumberFormat.currency(
      symbol: '₽',
      decimalDigits: 0,
    );

    return OpenContainer(
      closedElevation: 2,
      closedShape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(16),
      ),
      closedColor: color.withOpacity(0.1),
      openColor: Colors.white,
      openElevation: 8,
      openShape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(16),
      ),
      transitionDuration: const Duration(milliseconds: 400),
      openBuilder: (context, action, closedWidget) {
        return const SizedBox(); // Navigation handled by onTap
      },
      tappable: onTap != null,
      onTap: onTap,
      child: Container(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Container(
                  padding: const EdgeInsets.all(12),
                  decoration: BoxDecoration(
                    color: color.withOpacity(0.2),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: Icon(
                    icon,
                    color: color,
                    size: 24,
                  ),
                ),
                const Spacer(),
                _buildPercentageChange(context),
              ],
            ),
            const SizedBox(height: 16),
            Text(
              title,
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    color: Colors.grey[600],
                  ),
            ),
            const SizedBox(height: 8),
            FittedBox(
              child: Text(
                currencyFormat.format(value),
                style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                      color: color,
                      fontWeight: FontWeight.bold,
                    ),
              ),
            ),
            const SizedBox(height: 4),
            Text(
              subtitle,
              style: Theme.of(context).textTheme.bodySmall?.copyWith(
                    color: Colors.grey[600],
                  ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildPercentageChange(BuildContext context) {
    // Placeholder for percentage change indicator
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
      decoration: BoxDecoration(
        color: Colors.green.withOpacity(0.1),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Row(
        children: [
          const Icon(
            Icons.arrow_upward,
            size: 16,
            color: Colors.green,
          ),
          const SizedBox(width: 4),
          Text(
            '+5.2%',
            style: TextStyle(
              color: Colors.green,
              fontWeight: FontWeight.bold,
              fontSize: 12,
            ),
          ),
        ],
      ),
    );
  }
}
