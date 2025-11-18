#!/usr/bin/env python3
"""
Тест для проверки шифрования при добавлении новой платформы
"""
import os
import sys
sys.path.insert(0, '.')

# Устанавливаем переменные окружения
os.environ['FERNET_KEY'] = 'cF1T0jp6r0VfsbWgecSOOQCRLeUy813QrzqvvfyJcL0='

from app import create_app
from models import InvestmentPlatform
from extensions import db

def test_platform_encryption():
    """Тестируем шифрование при создании платформы"""
    app = create_app()
    
    with app.app_context():
        print("=== Тестирование шифрования API ключей ===")
        
        # Тестовые данные
        test_api_key = "test_api_key_12345"
        test_api_secret = "test_api_secret_67890"
        test_passphrase = "test_passphrase_abc"
        
        # Создаем новую платформу
        platform = InvestmentPlatform(
            name='Test Encryption',
            platform_type='crypto_exchange',
            api_key=test_api_key,
            api_secret=test_api_secret,
            passphrase=test_passphrase,
            is_active=True
        )
        
        print(f"Оригинальный API secret: {test_api_secret}")
        print(f"API secret после установки (property): {platform.api_secret[:20]}...")
        print(f"Значение в _api_secret (база): {platform._api_secret[:20] if platform._api_secret else None}...")
        
        print(f"\nОригинальный passphrase: {test_passphrase}")
        print(f"Passphrase после установки (property): {platform.passphrase[:10]}...")
        print(f"Значение в _passphrase (база): {platform._passphrase[:10] if platform._passphrase else None}...")
        
        # Проверяем, что данные в базе зашифрованы
        assert platform._api_secret != test_api_secret, "API secret должен быть зашифрован в базе"
        assert platform._passphrase != test_passphrase, "Passphrase должен быть зашифрован в базе"
        
        # Проверяем, что property возвращает расшифрованные данные
        assert platform.api_secret == test_api_secret, "Property должен возвращать расшифрованный API secret"
        assert platform.passphrase == test_passphrase, "Property должен возвращать расшифрованный passphrase"
        
        print("\n[OK] Все тесты шифрования пройдены!")
        
        # Сохраняем в базу и проверяем
        db.session.add(platform)
        db.session.commit()
        print("Платформа сохранена в базу данных")
        
        # Загружаем из базы и проверяем
        loaded_platform = InvestmentPlatform.query.filter_by(name='Test Encryption').first()
        print(f"Загружено из базы - API secret: {loaded_platform.api_secret[:20]}...")
        print(f"Загружено из базы - Passphrase: {loaded_platform.passphrase[:10]}...")
        
        assert loaded_platform.api_secret == test_api_secret, "После загрузки из базы API secret должен расшифровываться"
        assert loaded_platform.passphrase == test_passphrase, "После загрузки из базы passphrase должен расшифровываться"
        
        print("[OK] Тест загрузки из базы пройден!")
        
        # Удаляем тестовую платформу
        db.session.delete(loaded_platform)
        db.session.commit()
        print("Тестовая платформа удалена")

if __name__ == '__main__':
    test_platform_encryption()