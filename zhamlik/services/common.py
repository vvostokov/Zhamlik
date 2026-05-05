import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from flask import current_app, g
from extensions import db
from models import JsonCache, Category

def _get_currency_rates():
    """
    Возвращает словарь с курсами валют к рублю.
    Пытается получить актуальный курс USDT/RUB из кэша. Если кэш пуст,
    запрашивает курс напрямую. В случае ошибки использует значения по умолчанию.
    """
    if 'currency_rates' not in g:
        # Значения по умолчанию на случай, если API или кэш недоступны
        rates = {
            'USD': Decimal('90.0'), 
            'EUR': Decimal('100.0'), 
            'RUB': Decimal('1.0'), 
            'USDT': Decimal('90.0'), 
            None: Decimal('1.0') # Для активов без указания валюты
        }
        try:
            cache_entry = JsonCache.query.filter_by(cache_key='currency_rates').first()
            if cache_entry and cache_entry.json_data:
                cached_rates = json.loads(cache_entry.json_data)
                # Обновляем курсы из кэша, конвертируя строки в Decimal
                if 'USDT' in cached_rates:
                    rates['USDT'] = Decimal(cached_rates['USDT'])
                if 'USD' in cached_rates:
                    rates['USD'] = Decimal(cached_rates['USD'])
                current_app.logger.info(f"--- [Currency Rates] Курсы валют загружены из кэша: USDT={rates['USDT']}")
            else:
                # Если кэш пуст, пытаемся получить курс напрямую и создать кэш
                current_app.logger.info("--- [Currency Rates] Кэш курсов пуст, попытка получить свежий курс...")
                from api_clients import fetch_usdt_rub_rate # Локальный импорт для избежания циклической зависимости
                fresh_rate = fetch_usdt_rub_rate()
                if fresh_rate:
                    rates['USDT'] = fresh_rate
                    rates['USD'] = fresh_rate
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"--- [Currency Rates] Ошибка при получении курсов из кэша, используются значения по умолчанию. Ошибка: {e}")
        
        g.currency_rates = rates
    return g.currency_rates

from flask_login import current_user

def _get_or_create_category(name: str, type: str) -> Category:
    """Находит или создает категорию с заданным именем и типом."""
    # Search for category belonging to the user OR global (user_id=None)
    # Prefer user specific
    category = Category.query.filter_by(name=name, type=type, parent_id=None, user_id=current_user.id).first()
    
    if not category:
        # If not found, check if global exists (optional, depends on design. For now let's just create new user category)
        # category = Category.query.filter_by(name=name, type=type, parent_id=None, user_id=None).first()
        
        # if not category:
        category = Category(name=name, type=type, parent_id=None, user_id=current_user.id)
        db.session.add(category)
        # db.session.commit() # Не коммитим здесь, чтобы коммит был один в вызывающей функции
    return category
