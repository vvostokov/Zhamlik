import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/crypto_api_service.dart';
import '../models/crypto_transaction.dart';

class CryptoTransactionsScreen extends StatefulWidget {
  const CryptoTransactionsScreen({super.key});

  @override
  State<CryptoTransactionsScreen> createState() => _CryptoTransactionsScreenState();
}

class _CryptoTransactionsScreenState extends State<CryptoTransactionsScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '\$',
    decimalDigits: 2,
  );

  List<CryptoTransaction> _transactions = [];
  bool _isLoading = true;
  int _currentPage = 1;
  bool _hasNext = false;
  String _filterType = 'all';

  @override
  void initState() {
    super.initState();
    _loadTransactions();
  }

  Future<void> _loadTransactions({bool resetPage = false}) async {
    if (resetPage) {
      setState(() {
        _currentPage = 1;
      });
    }

    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final data = await apiService.getTransactions(
      page: _currentPage,
      filterType: _filterType,
    );

    if (mounted) {
      setState(() {
        if (data != null) {
          final transactionsJson = data['transactions'] as List? ?? [];
          _transactions = transactionsJson
              .map((json) => CryptoTransaction.fromJson(json as Map<String, dynamic>))
              .toList();
          _hasNext = data['has_next'] as bool? ?? false;
        }
        _isLoading = false;
      });
    }
  }

  void _loadNextPage() {
    if (_hasNext && !_isLoading) {
      setState(() {
        _currentPage++;
      });
      _loadTransactions();
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('История транзакций'),
        actions: [
          PopupMenuButton<String>(
            icon: const Icon(Icons.filter_list),
            onSelected: (value) {
              setState(() {
                _filterType = value;
              });
              _loadTransactions(resetPage: true);
            },
            itemBuilder: (context) => [
              const PopupMenuItem(
                value: 'all',
                child: Text('Все'),
              ),
              const PopupMenuItem(
                value: 'trade',
                child: Text('Обмен'),
              ),
              const PopupMenuItem(
                value: 'deposit',
                child: Text('Пополнение'),
              ),
              const PopupMenuItem(
                value: 'withdrawal',
                child: Text('Вывод'),
              ),
              const PopupMenuItem(
                value: 'transfer',
                child: Text('Перевод'),
              ),
            ],
          ),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : _transactions.isEmpty
              ? const Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Icon(Icons.receipt_long, size: 64, color: Colors.grey),
                      SizedBox(height: 16),
                      Text('Нет транзакций', style: TextStyle(fontSize: 18)),
                    ],
                  ),
                )
              : Column(
                  children: [
                    Expanded(
                      child: ListView.builder(
                        itemCount: _transactions.length + (_hasNext ? 1 : 0),
                        itemBuilder: (context, index) {
                          if (index == _transactions.length) {
                            // Load more indicator
                            _loadNextPage();
                            return const Center(
                              child: Padding(
                                padding: EdgeInsets.all(16),
                                child: CircularProgressIndicator(),
                              ),
                            );
                          }
                          return _buildTransactionCard(_transactions[index]);
                        },
                      ),
                    ),
                  ],
                ),
    );
  }

  Widget _buildTransactionCard(CryptoTransaction transaction) {
    return Card(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      child: ListTile(
        leading: _buildTransactionIcon(transaction.type),
        title: Text(
          transaction.typeDisplay,
          style: const TextStyle(fontWeight: FontWeight.w600),
        ),
        subtitle: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            if (transaction.platformName != null)
              Text(
                transaction.platformName!,
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 12,
                ),
              ),
            if (transaction.timestamp != null)
              Text(
                DateFormat('dd MMM yyyy, HH:mm').format(transaction.timestamp!),
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 12,
                ),
              ),
          ],
        ),
        trailing: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          crossAxisAlignment: CrossAxisAlignment.end,
          children: [
            if (transaction.asset1Ticker != null)
              Text(
                '${transaction.amount1.toStringAsFixed(4)} ${transaction.asset1Ticker!.toUpperCase()}',
                style: const TextStyle(
                  fontWeight: FontWeight.bold,
                ),
              ),
            if (transaction.asset2Ticker != null && transaction.amount2 > 0)
              Text(
                '→ ${transaction.amount2.toStringAsFixed(4)} ${transaction.asset2Ticker!.toUpperCase()}',
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 12,
                ),
              ),
          ],
        ),
      ),
    );
  }

  Widget _buildTransactionIcon(String type) {
    IconData icon;
    Color color;

    switch (type.toLowerCase()) {
      case 'deposit':
        icon = Icons.arrow_downward;
        color = Colors.green;
        break;
      case 'withdrawal':
        icon = Icons.arrow_upward;
        color = Colors.red;
        break;
      case 'trade':
        icon = Icons.swap_horiz;
        color = Colors.blue;
        break;
      case 'transfer':
        icon = Icons.send;
        color = Colors.orange;
        break;
      default:
        icon = Icons.receipt;
        color = Colors.grey;
    }

    return Container(
      width: 48,
      height: 48,
      decoration: BoxDecoration(
        color: color.withOpacity(0.1),
        borderRadius: BorderRadius.circular(12),
      ),
      child: Icon(icon, color: color),
    );
  }
}
