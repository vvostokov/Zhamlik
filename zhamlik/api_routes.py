import io
from flask import Blueprint, request, jsonify, current_app
from fns_client import parse_receipt_qr
from PIL import Image

import os
import sys

api_bp = Blueprint('api', __name__)
import pyzbar.pyzbar as pyzbar

print(f"Python executable: {sys.executable}")
print(f"sys.path: {sys.path}")

@api_bp.route('/parse-qr', methods=['POST'])
def handle_parse_qr():
    """
    API-эндпоинт для парсинга QR-кода чека.
    Принимает JSON с ключом 'qr_string'.
    Возвращает JSON с данными чека или ошибкой.

    Если qr_string не передан, ожидает получить изображение чека в multipart/form-data.
    """

    # Сначала проверяем JSON (для AJAX запросов)
    if request.is_json:
        data = request.get_json()
        qr_string = data.get('qr_string') if data else None
    # Затем проверяем form-data (для загрузки файлов)
    elif 'qr_string' in request.form:
        qr_string = request.form['qr_string']
    elif 'qr_image' in request.files:
        try:
            img = Image.open(io.BytesIO(request.files['qr_image'].read()))
            decoded_objects = pyzbar.decode(img)
            if decoded_objects:
                qr_string = decoded_objects[0].data.decode('utf-8')


        except Exception as e:
            return jsonify({'error': f'Ошибка обработки изображения: {str(e)}'}), 400

    # Проверяем, что qr_string был получен
    if not qr_string:
        return jsonify({'error': 'Необходимо передать qr_string или изображение чека.'}), 400

    # Проверяем наличие токена proverkacheka.com
    from fns_client import PROVERKACHEKA_TOKEN
    if not PROVERKACHEKA_TOKEN:
        return jsonify({'error': 'Сервис QR-кодов не настроен. Необходимо указать PROVERKACHEKA_TOKEN в .env файле.'}), 503

    try:
        parsed_data = parse_receipt_qr(qr_string)
        if parsed_data.get('error'):
            return jsonify(parsed_data), 400
        
        return jsonify(parsed_data), 200

    except Exception as e:
        current_app.logger.error(f"Непредвиденная ошибка при парсинге QR: {e}", exc_info=True)



        return jsonify({'error': f'Внутренняя ошибка сервера: {e}'}), 500