import os
import json
import time
import subprocess
from decimal import Decimal
from datetime import datetime
import requests

PROVERKACHEKA_HOST = "https://proverkacheka.com"
PROVERKACHEKA_ENDPOINT = "/api/v1/check/get"
PROVERKACHEKA_TOKEN = os.environ.get('PROVERKACHEKA_TOKEN', '33949.H1ArCQPQR5TWnmzuZ')

class FNSClient:
    def __init__(self, token=None):
        self.token = token or PROVERKACHEKA_TOKEN
        if not self.token:
             raise ValueError("Необходимо передать токен API proverkacheka.com.")

    def get_receipt(self, qr_string: str) -> dict:
        import urllib.parse
        data = "qrraw=" + urllib.parse.quote(qr_string) + "&token=" + self.token
        url = PROVERKACHEKA_HOST + PROVERKACHEKA_ENDPOINT
        
        cmd = ["/usr/bin/curl", "-s", "--max-time", "25", "-X", "POST", url, "-d", data]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.stdout:
            data = json.loads(result.stdout)
            return self._process_response(data)
        
        raise Exception("Пустой ответ от API чека")

    def _process_response(self, data: dict) -> dict:
        code = data.get('code')
        if code == 1:
            return data
        elif code == 0:
            raise Exception("Чек некорректен.")
        elif code == 2:
            raise Exception("Чек обрабатывается ФНС. Нажмите 'Повторить' через минуту.")
        elif code == 3:
            raise Exception("Превышено количество запросов к API.")
        elif code == 4:
            raise Exception("Необходимо ожидание перед повторным запросом. Подождите 1-2 минуты.")
        elif code == 5:
            raise Exception("Нет информации по чеку (возможно, чек не передан в ФНС).")
        else:
            raise Exception("Ошибка API (код " + str(code) + "): Данные не получены.")

def get_fns_client():
    return FNSClient(PROVERKACHEKA_TOKEN)

def parse_receipt_qr(qr_string: str) -> dict:
    if not qr_string or not qr_string.strip():
        raise ValueError("Строка QR-кода не может быть пустой.")
    try:
        client = get_fns_client()
        api_response = client.get_receipt(qr_string)
        receipt_json = api_response.get('data', {}).get('json', {})

        if not receipt_json:
             raise Exception("API вернул успешный код, но данные отсутствуют.")

        date_str = receipt_json.get('dateTime') or receipt_json.get('ticketDate')
        if isinstance(date_str, int):
             date_obj = datetime.fromtimestamp(date_str)
             date_formatted = date_obj.isoformat()
        else:
             date_formatted = date_str

        if date_formatted and 'T' in date_formatted:
            date_formatted = date_formatted[:16]

        total_sum = Decimal(str(receipt_json.get('totalSum', 0))) / 100

        parsed_data = {
            'date': date_formatted,
            'total_sum': total_sum,
            'merchant': receipt_json.get('user') or receipt_json.get('userInn'),
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
        print("Ошибка при обработке QR-кода: " + str(e))
        return {'error': str(e)}
