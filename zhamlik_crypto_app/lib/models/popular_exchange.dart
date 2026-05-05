class PopularExchange {
  final String name;
  final String displayName;
  final String logoEmoji;
  final String apiKeyHelpUrl;
  final List<String> keyInstructions;

  const PopularExchange({
    required this.name,
    required this.displayName,
    required this.logoEmoji,
    required this.apiKeyHelpUrl,
    required this.keyInstructions,
  });

  static const List<PopularExchange> all = [
    PopularExchange(
      name: 'Bybit',
      displayName: 'Bybit',
      logoEmoji: '🟡',
      apiKeyHelpUrl: 'https://www.bybit.com/app/user/api-management',
      keyInstructions: [
        '1. Зайдите на bybit.com и авторизуйтесь',
        '2. Перейдите в "API Management" (слева в меню)',
        '3. Нажмите "Create New Key"',
        '4. Введите название ключа и выберите права:',
        '   - ✓ Read (Чтение)',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Для безопасности используйте IP- whitelist и только read-only права!',
      ],
    ),
    PopularExchange(
      name: 'Binance',
      displayName: 'Binance',
      logoEmoji: '🟡',
      apiKeyHelpUrl: 'https://www.binance.com/en/my/settings/api-management',
      keyInstructions: [
        '1. Зайдите на binance.com и авторизуйтесь',
        '2. Перейдите в "API Management"',
        '3. Нажмите "Create API"',
        '4. Оставьте только "Enable Reading" (без торговли и вывода)',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Используйте только read-only права для безопасности!',
      ],
    ),
    PopularExchange(
      name: 'OKX',
      displayName: 'OKX',
      logoEmoji: '⚫',
      apiKeyHelpUrl: 'https://www.okx.com/account/my-api',
      keyInstructions: [
        '1. Зайдите на okx.com и авторизуйтесь',
        '2. Перейдите в "API" (Trade → API)',
        '3. Нажмите "Create API Key"',
        '4. Выберите "API Key" с правами "Read"',
        '5. Скопируйте API Key, Secret Key и Passphrase',
        '⚠️ Используйте только Trade-Read права! Не забудьте Passphrase!',
      ],
    ),
    PopularExchange(
      name: 'KuCoin',
      displayName: 'KuCoin',
      logoEmoji: '🔵',
      apiKeyHelpUrl: 'https://www.kucoin.com/hu/user/api',
      keyInstructions: [
        '1. Зайдите на kucoin.com и авторизуйтесь',
        '2. Перейдите в "API Management"',
        '3. Нажмите "Create API"',
        '4. ⚠️ ВАЖНО: Придумайте и запомните Passphrase (парольная фраза)!',
        '5. Выберите "General" + "Read Only"',
        '6. Скопируйте API Key, Secret и API Passphrase',
        '⚠️ Passphrase - ОБЯЗАТЕЛЬНОЕ поле для KuCoin! Без него не работает!',
      ],
    ),
    PopularExchange(
      name: 'Bitget',
      displayName: 'Bitget',
      logoEmoji: '🔵',
      apiKeyHelpUrl: 'https://www.bitget.com/api/user/auth',
      keyInstructions: [
        '1. Зайдите на bitget.com и авторизуйтесь',
        '2. Перейдите в "API Management"',
        '3. Нажмите "Create API"',
        '4. ⚠️ ВАЖНО: Придумайте и запомните Passphrase (парольная фраза)!',
        '5. Выберите "Read Only" права',
        '6. Установите IP whitelist для безопасности',
        '7. Скопируйте API Key, Secret Key и Passphrase',
        '⚠️ Passphrase - ОБЯЗАТЕЛЬНОЕ поле для Bitget!',
      ],
    ),
    PopularExchange(
      name: 'BingX',
      displayName: 'BingX',
      logoEmoji: '🔶',
      apiKeyHelpUrl: 'https://bingx.com/en-us/user/api-management',
      keyInstructions: [
        '1. Зайдите на bingx.com и авторизуйтесь',
        '2. Перейдите в "API Management"',
        '3. Нажмите "Create API"',
        '4. Выберите права "Read Only"',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Для безопасности используйте только read-only!',
      ],
    ),
    PopularExchange(
      name: 'Gate.io',
      displayName: 'Gate.io',
      logoEmoji: '🟢',
      apiKeyHelpUrl: 'https://www.gate.io/myaccount/apikey',
      keyInstructions: [
        '1. Зайдите на gate.io и авторизуйтесь',
        '2. Перейдите в "API Key"',
        '3. Нажмите "Create API Key"',
        '4. Установите метку (например, "Zhamlik")',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Используйте только "Read-only" права!',
      ],
    ),
    PopularExchange(
      name: 'HTX',
      displayName: 'HTX',
      logoEmoji: '🔴',
      apiKeyHelpUrl: 'https://www.htx.com/account/api',
      keyInstructions: [
        '1. Зайдите на htx.com и авторизуйтесь',
        '2. Перейдите в "API Management"',
        '3. Нажмите "Create API"',
        '4. Выберите "Read Only" разрешения',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Только чтение данных!',
      ],
    ),
    PopularExchange(
      name: 'Crypto.com',
      displayName: 'Crypto.com',
      logoEmoji: '🔵',
      apiKeyHelpUrl: 'https://crypto.com/exchange/settings/api',
      keyInstructions: [
        '1. Зайдите на crypto.com/exchange и авторизуйтесь',
        '2. Перейдите в "Settings" → "API"',
        '3. Нажмите "Create New Key"',
        '4. Заполните имя ключа и выберите "Read Only"',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Обязательно ограничьте права только для чтения!',
      ],
    ),
    PopularExchange(
      name: 'MEXC',
      displayName: 'MEXC',
      logoEmoji: '🟠',
      apiKeyHelpUrl: 'https://www.mexc.com/user/openapi',
      keyInstructions: [
        '1. Зайдите на mexc.com и авторизуйтесь',
        '2. Перейдите в "API Management"',
        '3. Нажмите "Create API"',
        '4. Выберите "Read Only" права',
        '5. Установите IP whitelist',
        '6. Скопируйте API Key и Secret Key',
        '⚠️ Только чтение!',
      ],
    ),
    PopularExchange(
      name: 'Kraken',
      displayName: 'Kraken',
      logoEmoji: '🐙',
      apiKeyHelpUrl: 'https://www.kraken.com/u/security/api',
      keyInstructions: [
        '1. Зайдите на kraken.com и авторизуйтесь',
        '2. Перейдите в "API"',
        '3. Нажмите "Create Key"',
        '4. Выберите "Read-only" для безопасности',
        '5. Скопируйте API Key и Secret Key',
        '⚠️ Используйте только Read-only разрешения!',
      ],
    ),
  ];
}
