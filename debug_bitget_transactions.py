#!/usr/bin/env python3
"""
Диагностика проблемы с синхронизацией транзакций Bitget
"""
import os
import sys
sys.path.insert(0, '.')

# Устанавливаем переменные окружения
os.environ['FERNET_KEY'] = 'cF1T0jp6r0VfsbWgecSOOQCRLeUy813QrzqvvfyJcL0='

from app import create_app
from models import InvestmentPlatform
from logic.platform_sync_logic import sync_platform_transactions
from api_clients import fetch_bitget_all_transactions
from datetime import datetime, timezone, timedelta

def debug_bitget_transactions():
    """Диагностируем синхронизацию транзакций Bitget"""
    app = create_app()
    
    with app.app_context():
        print("=== Диагностика синхронизации транзакций Bitget ===")
        
        # Находим платформу Bitget
        bitget_platform = InvestmentPlatform.query.filter_by(name='Bitget').first()
        if not bitget_platform:
            print("Платформа Bitget не найдена")
            return
        
        print(f"Платформа: {bitget_platform.name}")
        print(f"Последняя синхронизация транзакций: {bitget_platform.last_tx_synced_at}")
        
        # Тестируем получение транзакций за разные периоды
        end_time = datetime.now(timezone.utc)
        
        # Тест 1: За последние 7 дней
        print("\n--- Тест 1: Получение транзакций за последние 7 дней ---")
        start_time_7d = end_time - timedelta(days=7)
        try:
            txs_7d = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_7d,
                end_time,
                bitget_platform
            )
            print(f"Найдено транзакций за 7 дней: {sum(len(v) for v in txs_7d.values())}")
            for key, value in txs_7d.items():
                if value:
                    print(f"  {key}: {len(value)}")
                    if value and len(value) > 0:
                        print(f"    Последняя: {value[0].get('cTime', 'N/A')}")
        except Exception as e:
            print(f"Ошибка при получении транзакций за 7 дней: {e}")
            import traceback
            traceback.print_exc()
        
        # Тест 2: За последние 30 дней
        print("\n--- Тест 2: Получение транзакций за последние 30 дней ---")
        start_time_30d = end_time - timedelta(days=30)
        try:
            txs_30d = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_30d,
                end_time,
                bitget_platform
            )
            print(f"Найдено транзакций за 30 дней: {sum(len(v) for v in txs_30d.values())}")
            for key, value in txs_30d.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении транзакций за 30 дней: {e}")
        
        # Тест 3: За 2 года (как при первой синхронизации)
        print("\n--- Тест 3: Получение транзакций за 2 года (первая синхронизация) ---")
        start_time_2y = end_time - timedelta(days=2*365)
        try:
            txs_2y = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_2y,
                end_time,
                bitget_platform
            )
            print(f"Найдено транзакций за 2 года: {sum(len(v) for v in txs_2y.values())}")
            for key, value in txs_2y.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении транзакций за 2 года: {e}")
        
        # Тест 4: Без ограничений времени (может вызвать проблемы)
        print("\n--- Тест 4: Получение всех транзакций без временных ограничений ---")
        try:
            txs_all = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                None,
                None,
                bitget_platform
            )
            print(f"Найдено всех транзакций: {sum(len(v) for v in txs_all.values())}")
            for key, value in txs_all.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении всех транзакций: {e}")
        
        # Тест 5: Полная синхронизация через основной механизм
        print("\n--- Тест 5: Полная синхронизация через основной механизм ---")
        try:
            success, message = sync_platform_transactions(bitget_platform)
            print(f"Результат синхронизации: {success}")
            print(f"Сообщение: {message}")
        except Exception as e:
            print(f"Ошибка при синхронизации: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    debug_bitget_transactions()