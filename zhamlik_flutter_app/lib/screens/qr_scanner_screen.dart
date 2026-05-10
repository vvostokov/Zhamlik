import 'package:flutter/material.dart';
import 'package:mobile_scanner/mobile_scanner.dart';
import 'package:provider/provider.dart';
import 'package:permission_handler/permission_handler.dart';

import '../services/api_service.dart';
import '../models/account.dart';

class QRScannerScreen extends StatefulWidget {
  const QRScannerScreen({super.key});

  @override
  State<QRScannerScreen> createState() => _QRScannerScreenState();
}

class _QRScannerScreenState extends State<QRScannerScreen> {
  bool _isScanning = true;
  bool _isProcessing = false;
  bool _hasPermission = false;
  String? _scannedData;
  Map<String, dynamic>? _parsedReceipt;
  List<Account> _accounts = [];
  Account? _selectedAccount;

  double _parseDouble(dynamic value) {
    if (value == null) return 0.0;
    if (value is double) return value;
    if (value is int) return value.toDouble();
    if (value is String) return double.tryParse(value) ?? 0.0;
    return 0.0;
  }

  @override
  void initState() {
    super.initState();
    _requestCameraPermission();
    _loadAccounts();
  }

  Future<void> _requestCameraPermission() async {
    final status = await Permission.camera.request();
    setState(() {
      _hasPermission = status.isGranted;
    });

    if (!status.isGranted) {
      _showPermissionDialog();
    }
  }

  void _showPermissionDialog() {
    showDialog(
      context: context,
      barrierDismissible: false,
      builder: (context) => AlertDialog(
        title: const Text('Требуется разрешение'),
        content: const Text(
          'Для сканирования QR-кодов необходимо разрешить доступ к камере.',
        ),
        actions: [
          TextButton(
            onPressed: () {
              Navigator.of(context).pop();
              _requestCameraPermission();
            },
            child: const Text('Разрешить'),
          ),
          TextButton(
            onPressed: () {
              Navigator.of(context).pop();
              Navigator.of(context).pop();
            },
            child: const Text('Отмена'),
          ),
        ],
      ),
    );
  }

  Future<void> _loadAccounts() async {
    final apiService = Provider.of<ApiService>(context, listen: false);
    final accounts = await apiService.getAccounts();

    if (mounted) {
      setState(() {
        _accounts = accounts;
        _selectedAccount = accounts.isNotEmpty ? accounts.first : null;
      });
    }
  }

  void _onQRCodeDetected(String code) {
    if (_isProcessing) return;

    setState(() {
      _isScanning = false;
      _isProcessing = true;
      _scannedData = code;
    });

    _parseQRCode(code);
  }

  Future<void> _parseQRCode(String qrString) async {
    debugPrint('[QR DEBUG] Начало парсинга QR: $qrString');
    
    setState(() {
      _scannedData = qrString;
    });
    
    final apiService = Provider.of<ApiService>(context, listen: false);
    
    debugPrint('[QR DEBUG] Вызов API parseQR...');
    final result = await apiService.parseQR(qrString);
    
    debugPrint('[QR DEBUG] API вернул: $result');

    if (mounted) {
      setState(() {
        _isProcessing = false;
        _parsedReceipt = result;
      });

      if (result == null) {
        // Проверяем, есть ли сообщение об ошибке
        final errorMessage = 'Не удалось распознать QR-код. Проверьте интернет-соединение.';
        
        debugPrint('[QR DEBUG] Результат null - показываем ошибку: $errorMessage');

        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(errorMessage),
            backgroundColor: Colors.red,
            duration: const Duration(seconds: 3),
          ),
        );

        setState(() {
          _isScanning = true;
        });
      } else if (result.containsKey('error')) {
        final errorMessage = result['error'] ?? 'Ошибка сервера';
        debugPrint('[QR DEBUG] API вернул ошибку: $errorMessage');
        
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(errorMessage),
            backgroundColor: Colors.orange,
            duration: const Duration(seconds: 4),
          ),
        );
        
        // Если ошибка о повторном запросе - предлагаем повторить
        if (errorMessage.contains('ожидание') || errorMessage.contains('повторить')) {
          // Оставим результат для повторной попытки
        } else {
          setState(() {
            _isScanning = true;
          });
        }
      } else {
        debugPrint('[QR DEBUG] Успех! Данные получены');
      }
    }
  }

  Future<void> _createTransactionFromReceipt() async {
    if (_parsedReceipt == null || _selectedAccount == null) {
      return;
    }

    setState(() {
      _isProcessing = true;
    });

    final apiService = Provider.of<ApiService>(context, listen: false);

    // Извлекаем данные из data
    final receiptData = _parsedReceipt!.containsKey('data')
        ? _parsedReceipt!['data'] as Map<String, dynamic>
        : _parsedReceipt!;

    final result = await apiService.receiptToTransaction(
      receiptData: receiptData,
      accountId: _selectedAccount!.id,
    );

    if (mounted) {
      setState(() {
        _isProcessing = false;
      });

      if (result != null) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Операция успешно добавлена!'),
            backgroundColor: Colors.green,
          ),
        );
        Navigator.of(context).pop(true); // Return true to indicate success
      } else {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Ошибка при создании операции'),
            backgroundColor: Colors.red,
          ),
        );
      }
    }
  }

  void _resetScanner() {
    setState(() {
      _isScanning = true;
      _scannedData = null;
      _parsedReceipt = null;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Сканировать чек'),
      ),
      body: _buildBody(),
    );
  }

  Widget _buildBody() {
    if (_isScanning) {
      return _buildScanner();
    } else if (_parsedReceipt != null) {
      return _buildReceiptPreview();
    } else {
      return _buildScanningResult();
    }
  }

  Widget _buildScanner() {
    if (!_hasPermission) {
      return const Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            CircularProgressIndicator(),
            SizedBox(height: 16),
            Text('Ожидание разрешения камеры...'),
          ],
        ),
      );
    }

    return Column(
      children: [
        Expanded(
          child: MobileScanner(
            onDetect: (capture) {
              final code = capture.barcodes.first.rawValue;
              if (code != null) {
                _onQRCodeDetected(code);
              }
            },
            errorBuilder: (context, error, child) {
              return Center(
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    const Icon(Icons.error, size: 64, color: Colors.red),
                    const SizedBox(height: 16),
                    Text('Ошибка камеры: ${error.toString()}'),
                    const SizedBox(height: 16),
                    ElevatedButton(
                      onPressed: () {
                        setState(() {});
                      },
                      child: const Text('Повторить'),
                    ),
                  ],
                ),
              );
            },
          ),
        ),
        Container(
          padding: const EdgeInsets.all(24),
          child: Column(
            children: [
              const Icon(
                Icons.qr_code_scanner,
                size: 48,
                color: Colors.white,
              ),
              const SizedBox(height: 16),
              Text(
                'Наведите камеру на QR-код чека',
                style: Theme.of(context).textTheme.titleLarge?.copyWith(
                      color: Colors.white,
                    ),
                textAlign: TextAlign.center,
              ),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildScanningResult() {
    return Center(
      child: Padding(
        padding: const EdgeInsets.all(24),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            if (_isProcessing)
              const CircularProgressIndicator()
            else
              const Icon(Icons.error, size: 64, color: Colors.red),
            const SizedBox(height: 16),
            Text(
              _isProcessing ? 'Обработка...' : 'Не удалось распознать чек',
              style: Theme.of(context).textTheme.titleLarge,
            ),
            const SizedBox(height: 24),
            ElevatedButton.icon(
              onPressed: _resetScanner,
              icon: const Icon(Icons.refresh),
              label: const Text('Сканировать снова'),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildReceiptPreview() {
    final receipt = _parsedReceipt!;

    // API возвращает {success: true, data: {...}}
    // Извлекаем данные из data
    final data = receipt.containsKey('data')
        ? receipt['data'] as Map<String, dynamic>
        : receipt;

    final total = data['total_sum'] ?? 0.0;
    final date = data['date'] ?? '';
    final merchant = data['merchant'] ?? 'Магазин';
    final items = data['items'] as List? ?? [];

    return SingleChildScrollView(
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Card(
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    merchant,
                    style: Theme.of(context).textTheme.titleLarge?.copyWith(
                          fontWeight: FontWeight.bold,
                        ),
                  ),
                  const SizedBox(height: 8),
                  Text(
                    date,
                    style: TextStyle(color: Colors.grey[600]),
                  ),
                  const SizedBox(height: 16),
                  Text(
                    'Итого: ${_parseDouble(total).toStringAsFixed(2)} ₽',
                    style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                          color: Theme.of(context).colorScheme.primary,
                          fontWeight: FontWeight.bold,
                        ),
                  ),
                ],
              ),
            ),
          ),
          const SizedBox(height: 16),
          if (items.isNotEmpty) ...[
            Text(
              'Товары:',
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 8),
            Card(
              child: Column(
                children: items.map<Widget>((item) {
                  return ListTile(
                    title: Text(item['name'] ?? 'Товар'),
                    subtitle: Text(
                        '${item['quantity']} x ${_parseDouble(item['price']).toStringAsFixed(2)} ₽'),
                    trailing: Text(
                      '${_parseDouble(item['sum']).toStringAsFixed(2)} ₽',
                      style: const TextStyle(fontWeight: FontWeight.bold),
                    ),
                  );
                }).toList(),
              ),
            ),
            const SizedBox(height: 16),
          ],
          Text(
            'Счет для списания:',
            style: Theme.of(context).textTheme.titleMedium?.copyWith(
                  fontWeight: FontWeight.bold,
                ),
          ),
          const SizedBox(height: 8),
          DropdownButtonFormField<Account>(
            value: _selectedAccount,
            decoration: const InputDecoration(
              border: OutlineInputBorder(),
              contentPadding: EdgeInsets.symmetric(
                horizontal: 12,
                vertical: 16,
              ),
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
              setState(() {
                _selectedAccount = account;
              });
            },
          ),
          const SizedBox(height: 24),
          Row(
            children: [
              Expanded(
                child: OutlinedButton(
                  onPressed: _resetScanner,
                  child: const Text('Отмена'),
                ),
              ),
              const SizedBox(width: 16),
              Expanded(
                child: ElevatedButton(
                  onPressed: _isProcessing ? null : _createTransactionFromReceipt,
                  child: _isProcessing
                      ? const SizedBox(
                          height: 20,
                          width: 20,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Text('Добавить'),
                ),
              ),
            ],
          ),
        ],
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
        return Icons.credit_card;
      case 'deposit':
        return Icons.savings;
      case 'cash':
        return Icons.money;
      default:
        return Icons.account_balance_wallet;
    }
  }
}
