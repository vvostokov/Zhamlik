#!/usr/bin/env python3
"""
Проверка работы существующих платформ после исправления
"""
import os
import sys
sys.path.insert(0, '.')

# Устанавливаем переменные окружения
os.environ['FERNET_KEY'] = 'cF1T0jp6r0VfsbWgecSOOQCRLeUy813QrzqvvfyJcL0='

from app import create_app
from models import InvestmentPlatform
from logic.platform_sync_logic import sync_platform_balances, sync_platform_transactions

def test_existing_platforms():
    """Проверяем работу существующих платформ"""
    app = create_app()
    
    with app.app_context():
        print("=== Проверка существующих платформ ===")
        
        platforms = InvestmentPlatform.query.filter_by(is_active=True).all()
        print(f"Найдено активных платформ: {len(platforms)}")
        
        for platform in platforms:
            print(f"\n--- Платформа: {platform.name} ---")
            print(f"Тип: {platform.platform_type}")
            print(f"API Key: {platform.api_key[:10] if platform.api_key else None}...")
            print(f"Has API Secret: {bool(platform.api_secret)}")
            print(f"Has Passphrase: {bool(platform.passphrase)}")
            
            # Пробуем синхронизировать балансы
            try:
                success, message = sync_platform_balances(platform)
                print(f"Синхронизация балансов: {'Успешно' if success else 'Ошибка'}")
                print(f"Сообщение: {message}")
                
                if success:
                    assets_count = platform.assets.filter(InvestmentAsset.quantity > 0).count()
                    print(f"Активов с балансом: {assets_count}")
                    
            except Exception as e:
                print(f"Ошибка при синхронизации балансов: {e}")
            
            # Пробуем синхронизировать транзакции
            try:
                success, message = sync_platform_transactions(platform)
                print(f"Синхронизация транзакций: {'Успешно' if success else 'Ошибка'}")
                print(f"Сообщение: {message}")
                
            except Exception as e:
                print(f"Ошибка при синхронизации транзакций: {e}")

if __name__ == '__main__':
    test_existing_platforms()