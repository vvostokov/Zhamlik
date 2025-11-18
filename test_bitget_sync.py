#!/usr/bin/env python3
"""
Тест для проверки исправленной синхронизации с Bitget
"""
import os
import sys
sys.path.insert(0, '.')

# Устанавливаем переменные окружения
os.environ['FERNET_KEY'] = 'cF1T0jp6r0VfsbWgecSOOQCRLeUy813QrzqvvfyJcL0='

from app import create_app
from models import InvestmentPlatform, InvestmentAsset
from logic.platform_sync_logic import sync_platform_balances
from api_clients import fetch_bitget_account_assets

def test_bitget_sync():
    """Тестируем синхронизацию с Bitget"""
    app = create_app()
    
    with app.app_context():
        print("=== Тестирование синхронизации с Bitget ===")
        
        # Проверяем существующие платформы
        platforms = InvestmentPlatform.query.all()
        print(f"Найдено платформ: {len(platforms)}")
        
        bitget_platform = None
        for p in platforms:
            print(f"- {p.name}: {p.platform_type}")
            if p.name.lower() == 'bitget':
                bitget_platform = p
        
        if not bitget_platform:
            print("Создаем тестовую платформу Bitget...")
            # Используем ключи из файла bitget.py
            from cripto.bitget import API_KEY, API_SECRET, PASSPHRASE
            
            bitget_platform = InvestmentPlatform(
                name='Bitget',
                platform_type='crypto_exchange',
                api_key=API_KEY,
                api_secret=API_SECRET,
                passphrase=PASSPHRASE,
                is_active=True
            )
            
            # Проверяем автоматическое шифрование
            print(f"API secret после установки: {bitget_platform.api_secret[:20] if bitget_platform.api_secret else None}...")
            print(f"Passphrase после установки: {bitget_platform.passphrase[:10] if bitget_platform.passphrase else None}...")
            
            from extensions import db
            db.session.add(bitget_platform)
            db.session.commit()
            print("Платформа Bitget создана")
        
        # Тестируем синхронизацию
        print("\n=== Тестирование синхронизации балансов ===")
        try:
            success, message = sync_platform_balances(bitget_platform)
            print(f"Результат синхронизации: {success}")
            print(f"Сообщение: {message}")
            
            if success:
                print("Балансы после синхронизации:")
                for asset in bitget_platform.assets.filter(InvestmentAsset.quantity > 0):
                    print(f"  {asset.ticker}: {asset.quantity} ({asset.source_account_type})")
            
        except Exception as e:
            print(f"Ошибка при синхронизации: {e}")
            import traceback
            traceback.print_exc()
        
        # Тестируем прямой API вызов
        print("\n=== Тестирование прямого API вызова ===")
        try:
            assets = fetch_bitget_account_assets(
                bitget_platform.api_key,
                bitget_platform.api_secret, 
                bitget_platform.passphrase
            )
            print(f"Получено активов через API: {len(assets)}")
            for asset in assets[:3]:
                print(f"  {asset['ticker']}: {asset['quantity']} ({asset.get('account_type', 'Unknown')})")
        except Exception as e:
            print(f"Ошибка прямого API вызова: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    test_bitget_sync()