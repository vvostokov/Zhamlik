import os
import json
import time
from decimal import Decimal
from datetime import datetime
import requests

# --- Константы для нового API proverkacheka.com ---
PROVERKACHEKA_HOST = "https://proverkacheka.com"
PROVERKACHEKA_ENDPOINT = "/api/v1/check/get"

# Токен доступа, который может быть задан в переменных окружения или передан напрямую
PROVERKACHEKA_TOKEN = os.environ.get('PROVERKACHEKA_TOKEN', '33949.H1ArCQPQR5TWnmzuZ') 

class FNSClient:
    """Клиент для взаимодействия с API proverkacheka.com (вместо прямого API ФНС)."""
    def __init__(self, token=None):
        self.token = token or PROVERKACHEKA_TOKEN
        if not self.token:
             raise ValueError("Необходимо передать токен API proverkacheka.com.")
        self.session = requests.Session()

    def get_receipt(self, qr_string: str) -> dict:
        """Получает детали чека по строке из QR-кода через API proverkacheka.com."""
        
        # Используем Формат запроса 2: передача сырой строки qrraw
        payload = {
            'qrraw': qr_string,
            'token': self.token
        }
        
        try:
            response = self.session.post(PROVERKACHEKA_HOST + PROVERKACHEKA_ENDPOINT, data=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Обработка кодов ответа согласно документации
            code = data.get('code')
            if code == 1:
                return data # Успех
            elif code == 0:
                 raise Exception("Чек некорректен.")
            elif code == 2:
                 # Данные пока не получены, API вернет их позже? 
                 # В данном синхронном контексте это неудача, но можно реализовать ожидание.
                 # Документация не говорит о поллинге для формата 2, но обычно это так.
                 # Однако, для простоты вернем ошибку, чтобы пользователь попробовал позже.
                 raise Exception("Данные чека еще обрабатываются. Попробуйте позже.")
            elif code == 3:
                 raise Exception("Превышено количество запросов к API.")
            elif code == 4:
                 raise Exception("Необходимо ожидание перед повторным запросом.")
            else:
                 raise Exception(f"Ошибка API (код {code}): Данные не получены.")

        except requests.RequestException as e:
            raise Exception(f"Ошибка запроса к API proverkacheka.com: {e}")

def get_fns_client():
    """
    Фабричная функция для получения экземпляра FNSClient.
    Кэширование здесь менее критично, так как нет сложной авторизации, но оставим для совместимости.
    """
    return FNSClient(PROVERKACHEKA_TOKEN)

def parse_receipt_qr(qr_string: str) -> dict:
    """Парсит строку QR-кода с чека и возвращает структурированные данные."""
    if not qr_string or not qr_string.strip():
        raise ValueError("Строка QR-кода не может быть пустой.")
    try:
        client = get_fns_client()
        api_response = client.get_receipt(qr_string)
        
        # Структура ответа: data -> json -> ...
        receipt_json = api_response.get('data', {}).get('json', {})
        
        if not receipt_json:
             raise Exception("API вернул успешный код, но данные отсутствуют.")

        # Преобразование формата даты "2025-11-19T15:30:00" или timestamp
        date_str = receipt_json.get('dateTime') # В примере документации ticketDate, но в JSON обычно dateTime или ticketDate
        if not date_str:
             date_str = receipt_json.get('ticketDate') # Пробуем альтернативное поле
        
        # Если дата пришла в iso формате, оставляем. Если timestamp - конвертируем.
        # API обычно возвращает ISO строку или timestamp.
        # В документации: data.json.ticketDate	Дата
        # Посмотрим на формат.
        # Если это timestamp (int):
        if isinstance(date_str, int):
             date_obj = datetime.fromtimestamp(date_str)
             date_formatted = date_obj.isoformat()
        else:
             # Предположим, что это строка, попробуем распарсить, если нужно, или вернуть как есть
             # Для совместимости с фронтендом лучше вернуть ISO строку.
             # Часто приходит '2020-09-24T18:37:00'.
             date_formatted = date_str
        
        total_sum = Decimal(str(receipt_json.get('totalSum', 0))) / 100 # Сумма в копейках -> рубли

        parsed_data = {
            'date': date_formatted,
            'total_sum': total_sum,
            'merchant': receipt_json.get('user') or receipt_json.get('userInn'), # Организация или ИНН
            'items': [],
            'error': None
        }
        
        for item in receipt_json.get('items', []):
            parsed_data['items'].append({
                'name': item.get('name', 'Товар'),
                'quantity': Decimal(str(item.get('quantity', 1))),
                'price': Decimal(str(item.get('price', 0))) / 100,
                'total': Decimal(str(item.get('sum', 0))) / 100,
            })
            
        return parsed_data

    except Exception as e:
        print(f"Ошибка при обработке QR-кода: {e}")
        return {'error': str(e)}