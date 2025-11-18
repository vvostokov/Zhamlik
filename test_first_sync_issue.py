#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест для диагностики проблемы с первой синхронизацией транзакций Bitget
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

def test_first_sync_issue():
    """Диагностируем проблему с первой синхронизацией транзакций Bitget"""
    app = create_app()
    
    with app.app_context():
        print("=== Диагностика проблемы с первой синхронизацией транзакций Bitget ===")
        
        # Находим платформу Bitget
        bitget_platform = InvestmentPlatform.query.filter_by(name='Bitget').first()
        if not bitget_platform:
            print("Платформа Bitget не найдена")
            return
        
        print(f"Платформа: {bitget_platform.name}")
        print(f"Последняя синхронизация транзакций: {bitget_platform.last_tx_synced_at}")
        print(f"API Key: {bitget_platform.api_key[:10] if bitget_platform.api_key else None}...")
        print(f"Has API Secret: {bool(bitget_platform.api_secret)}")
        print(f"Has Passphrase: {bool(bitget_platform.passphrase)}")
        
        # Тестируем получение транзакций за разные периоды
        end_time = datetime.now(timezone.utc)
        
        # Тест 1: За последние 30 дней (как при последующих синхронизациях)
        print("\n--- Тест 1: Получение транзакций за последние 30 дней ---")
        start_time_30d = end_time - timedelta(days=30)
        try:
            txs_30d = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_dt=start_time_30d,
                end_time_dt=end_time,
                platform=bitget_platform
            )
            print(f"Транзакций за 30 дней: {sum(len(v) for v in txs_30d.values())}")
            for key, value in txs_30d.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении транзакций за 30 дней: {e}")
        
        # Тест 2: За последние 90 дней
        print("\n--- Тест 2: Получение транзакций за последние 90 дней ---")
        start_time_90d = end_time - timedelta(days=90)
        try:
            txs_90d = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_dt=start_time_90d,
                end_time_dt=end_time,
                platform=bitget_platform
            )
            print(f"Транзакций за 90 дней: {sum(len(v) for v in txs_90d.values())}")
            for key, value in txs_90d.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении транзакций за 90 дней: {e}")
        
        # Тест 3: За 2 года (как при первой синхронизации)
        print("\n--- Тест 3: Получение транзакций за 2 года (первая синхронизация) ---")
        start_time_2y = end_time - timedelta(days=2*365)
        try:
            txs_2y = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_dt=start_time_2y,
                end_time_dt=end_time,
                platform=bitget_platform
            )
            print(f"Транзакций за 2 года: {sum(len(v) for v in txs_2y.values())}")
            for key, value in txs_2y.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении транзакций за 2 года: {e}")
        
        # Тест 4: Без ограничений времени (может вызвать проблемы)
        print("\n--- Тест 4: Получение всех транзакций без ограничений ---")
        try:
            txs_all = fetch_bitget_all_transactions(
                bitget_platform.api_key,
                bitget_platform.api_secret,
                bitget_platform.passphrase,
                start_time_dt=None,  # Без ограничения
                end_time_dt=end_time,
                platform=bitget_platform
            )
            print(f"Всех транзакций без ограничений: {sum(len(v) for v in txs_all.values())}")
            for key, value in txs_all.items():
                if value:
                    print(f"  {key}: {len(value)}")
        except Exception as e:
            print(f"Ошибка при получении всех транзакций: {e}")

if __name__ == '__main__':
    test_first_sync_issue()