import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/api_service.dart';
import '../models/transaction.dart';
import '../models/account.dart';
import 'home_screen.dart';

class TransactionsScreen extends StatefulWidget {
  const TransactionsScreen({super.key});

  @override
  State<TransactionsScreen> createState() => _TransactionsScreenState();
}

class _TransactionsScreenState extends State<TransactionsScreen> {
  final DateFormat dateFormatter = DateFormat('dd MMM yyyy, HH:mm');
  List<Transaction> _transactions = [];
  List<Account> _accounts = [];
  bool _isLoading = true;
  String? _selectedType;
  int _currentPage = 1;
  int _totalPages = 1;

  @override
  void initState() {
    super.initState();
    _loadData();
  }

  Future<void> _loadData() async {
    await Future.wait([
      _loadTransactions(),
      _loadAccounts(),
    ]);
  }

  Future<void> _loadAccounts() async {
    final apiService = Provider.of<ApiService>(context, listen: false);
    final accounts = await apiService.getAccounts();
    if (mounted) {
      setState(() {
        _accounts = accounts;
      });
    }
  }

  Future<void> _loadTransactions() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<ApiService>(context, listen: false);
    final transactions = await apiService.getTransactions(
      page: _currentPage,
      type: _selectedType,
    );

    if (mounted) {
      setState(() {
        _transactions = transactions;
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Операции'),
      ),
      body: Column(
        children: [
          _buildFilterBar(),
          Expanded(
            child: _isLoading
                ? const Center(child: CircularProgressIndicator())
                : _transactions.isEmpty
                    ? const Center(child: Text('Нет операций'))
                    : RefreshIndicator(
                        onRefresh: _loadTransactions,
                        child: ListView.builder(
                          itemCount: _transactions.length,
                          itemBuilder: (context, index) {
                            return _buildTransactionItem(
                              _transactions[index],
                              index == _transactions.length - 1,
                            );
                          },
                        ),
                      ),
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _showAddTransactionDialog(context),
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildFilterBar() {
    return Container(
      padding: const EdgeInsets.all(16),
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: Row(
          children: [
            FilterChip(
              label: const Text('Все'),
              selected: _selectedType == null,
              onSelected: (selected) {
                setState(() {
                  _selectedType = selected ? null : _selectedType;
                  _currentPage = 1;
                });
                _loadTransactions();
              },
            ),
            const SizedBox(width: 8),
            FilterChip(
              label: const Text('Доходы'),
              selected: _selectedType == 'income',
              onSelected: (selected) {
                setState(() {
                  _selectedType = selected ? 'income' : null;
                  _currentPage = 1;
                });
                _loadTransactions();
              },
            ),
            const SizedBox(width: 8),
            FilterChip(
              label: const Text('Расходы'),
              selected: _selectedType == 'expense',
              onSelected: (selected) {
                setState(() {
                  _selectedType = selected ? 'expense' : null;
                  _currentPage = 1;
                });
                _loadTransactions();
              },
            ),
            const SizedBox(width: 8),
            FilterChip(
              label: const Text('Переводы'),
              selected: _selectedType == 'transfer',
              onSelected: (selected) {
                setState(() {
                  _selectedType = selected ? 'transfer' : null;
                  _currentPage = 1;
                });
                _loadTransactions();
              },
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildTransactionItem(Transaction tx, bool isLast) {
    final isIncome = tx.type == 'income';
    final isTransfer = tx.type == 'transfer';
    final color = isTransfer ? Colors.blue : (isIncome ? Colors.green : Colors.red); // Синий - переводы, зеленый - доходы, красный - расходы

    return Column(
      children: [
        ListTile(
          leading: Container(
            width: 40,
            height: 40,
            decoration: BoxDecoration(
              color: color.withOpacity(0.1),
              borderRadius: BorderRadius.circular(8),
            ),
            child: Icon(
              isTransfer ? Icons.swap_horiz : (isIncome ? Icons.arrow_downward : Icons.arrow_upward),
              color: color,
            ),
          ),
          title: Text(
            tx.description ?? tx.merchant ?? 'Без названия',
            style: const TextStyle(fontWeight: FontWeight.w500),
          ),
          subtitle: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                dateFormatter.format(tx.date),
                style: TextStyle(color: Colors.grey[600], fontSize: 12),
              ),
              if (tx.items != null && tx.items!.isNotEmpty)
                Text(
                  '${tx.items!.length} товаров',
                  style: TextStyle(color: Colors.grey[600], fontSize: 11),
                ),
            ],
          ),
          trailing: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              Text(
                '${isIncome ? '+' : '-'}${tx.amount.toStringAsFixed(2)} ₽',
                style: TextStyle(
                  color: color,
                  fontWeight: FontWeight.bold,
                  fontSize: 16,
                ),
              ),
            ],
          ),
        ),
        if (tx.items != null && tx.items!.isNotEmpty)
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: Text(
                    'Товары:',
                    style: Theme.of(context).textTheme.labelSmall,
                  ),
                ),
                ...tx.items!.take(3).map((item) {
                  return Padding(
                    padding: const EdgeInsets.symmetric(vertical: 2),
                    child: Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        Expanded(
                          child: Text(
                            item.name,
                            style: const TextStyle(fontSize: 12),
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                          ),
                        ),
                        Text(
                          '${item.quantity} x ${item.price.toStringAsFixed(2)} ₽',
                          style: const TextStyle(fontSize: 12),
                        ),
                      ],
                    ),
                  );
                }),
                if (tx.items!.length > 3)
                  Padding(
                    padding: const EdgeInsets.only(top: 4),
                    child: Text(
                      'И еще ${tx.items!.length - 3}...',
                      style: const TextStyle(fontSize: 11, color: Colors.grey),
                    ),
                  ),
              ],
            ),
          ),
        if (!isLast) const Divider(height: 1),
      ],
    );
  }

  void _showAddTransactionDialog(BuildContext context) {
    final amountController = TextEditingController();
    final descriptionController = TextEditingController();
    // Set initial type based on current filter
    String _transactionType = _selectedType ?? 'expense';
    Account? _selectedAccount = _accounts.isNotEmpty ? _accounts.first : null;
    Account? _toAccount; // For transfers

    showDialog(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) => AlertDialog(
          title: const Text('Добавить операцию'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                SegmentedButton<String>(
                  segments: const [
                    ButtonSegment(
                      value: 'income',
                      label: Text('Доход'),
                      icon: Icon(Icons.arrow_downward),
                    ),
                    ButtonSegment(
                      value: 'expense',
                      label: Text('Расход'),
                      icon: Icon(Icons.arrow_upward),
                    ),
                    ButtonSegment(
                      value: 'transfer',
                      label: Text('Перевод'),
                      icon: Icon(Icons.swap_horiz),
                    ),
                  ],
                  selected: {_transactionType},
                  onSelectionChanged: (Set<String> selected) {
                    setDialogState(() {
                      _transactionType = selected.first;
                    });
                  },
                ),
                const SizedBox(height: 16),
                DropdownButtonFormField<Account>(
                  value: _selectedAccount,
                  decoration: InputDecoration(
                    labelText: _transactionType == 'transfer' ? 'От счёта' : 'Счет',
                    prefixIcon: const Icon(Icons.account_balance_wallet),
                    border: const OutlineInputBorder(),
                  ),
                  items: _accounts.map((account) {
                    return DropdownMenuItem(
                      value: account,
                      child: Row(
                        children: [
                          Icon(_getAccountIcon(account.type)),
                          const SizedBox(width: 12),
                          Expanded(
                            child: Text(
                              account.name,
                              style: const TextStyle(fontSize: 14),
                            ),
                          ),
                          Text(
                            '${account.balance.toStringAsFixed(2)} ${account.currency}',
                            style: TextStyle(
                              fontSize: 12,
                              color: Colors.grey[600],
                            ),
                          ),
                        ],
                      ),
                    );
                  }).toList(),
                  onChanged: (account) {
                    setDialogState(() {
                      _selectedAccount = account;
                    });
                  },
                ),
                if (_transactionType == 'transfer') ...[
                  const SizedBox(height: 16),
                  DropdownButtonFormField<Account>(
                    value: _toAccount,
                    decoration: const InputDecoration(
                      labelText: 'На счёт',
                      prefixIcon: Icon(Icons.account_balance_wallet),
                      border: OutlineInputBorder(),
                    ),
                    items: _accounts.map((account) {
                      return DropdownMenuItem(
                        value: account,
                        child: Row(
                          children: [
                            Icon(_getAccountIcon(account.type)),
                            const SizedBox(width: 12),
                            Expanded(
                              child: Text(
                                account.name,
                                style: const TextStyle(fontSize: 14),
                              ),
                            ),
                            Text(
                              '${account.balance.toStringAsFixed(2)} ${account.currency}',
                              style: TextStyle(
                                fontSize: 12,
                                color: Colors.grey[600],
                              ),
                            ),
                          ],
                        ),
                      );
                    }).toList(),
                    onChanged: (account) {
                      setDialogState(() {
                        _toAccount = account;
                      });
                    },
                  ),
                ],
                const SizedBox(height: 16),
                TextField(
                  controller: amountController,
                  decoration: const InputDecoration(
                    labelText: 'Сумма',
                    prefixIcon: Icon(Icons.money),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.number,
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: descriptionController,
                  decoration: const InputDecoration(
                    labelText: 'Описание',
                    prefixIcon: Icon(Icons.description),
                    border: OutlineInputBorder(),
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
                final amount = double.tryParse(amountController.text);
                if (amount == null || amount <= 0) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Введите корректную сумму')),
                  );
                  return;
                }
                if (_selectedAccount == null) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Создайте счет для добавления операции')),
                  );
                  return;
                }
                if (_transactionType == 'transfer' && _toAccount == null) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Выберите счёт для перевода')),
                  );
                  return;
                }
                if (_transactionType == 'transfer' && _selectedAccount == _toAccount) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Нельзя переводить на тот же счёт')),
                  );
                  return;
                }

                Navigator.of(context).pop();

                final apiService = Provider.of<ApiService>(context, listen: false);
                final result = await apiService.createTransaction(
                  amount: amount,
                  type: _transactionType,
                  accountId: _selectedAccount!.id,
                  toAccountId: _toAccount?.id,
                  description: descriptionController.text.isNotEmpty
                      ? descriptionController.text
                      : null,
                );

                if (result != null) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Операция добавлена'),
                      backgroundColor: Colors.green,
                    ),
                  );
                  _loadData();
                } else {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Ошибка при добавлении операции'),
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

  IconData _getAccountIcon(String type) {
    switch (type) {
      case 'bank_account':
        return Icons.account_balance;
      case 'bank_card':
        return Icons.credit_card;
      case 'credit':
        return Icons.credit_score;
      case 'deposit':
        return Icons.savings;
      case 'cash':
        return Icons.money;
      case 'ewallet':
        return Icons.account_balance_wallet;
      default:
        return Icons.account_balance;
    }
  }
}
