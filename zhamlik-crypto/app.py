from flask import Flask, render_template, request, jsonify, send_from_directory, session
from datetime import datetime, timedelta
import os
import requests

app = Flask(__name__)

# Configuration
# Используем тот же SECRET_KEY что и основной Zhamlik для общей сессии
app.config['SECRET_KEY'] = os.environ.get('FERNET_KEY', os.environ.get('SECRET_KEY', 'crypto-secret-key-change-in-production'))

# Настройки сессии для работы с основным Zhamlik
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_NAME'] = 'session'
app.config['SESSION_COOKIE_DOMAIN'] = None  # Работает для текущего домена (включая разные порты)
app.config['SESSION_COOKIE_PATH'] = '/'      # Доступно для всех путей

# API Configuration - основной Zhamlik
# Для локальной разработки: http://localhost:5000
# Для production на том же сервере: http://localhost:8888 (через nginx)
ZHAMLIK_API_URL = os.environ.get('ZHAMLIK_API_URL', 'http://localhost:8888')

# Shared secret для internal API
INTERNAL_API_SECRET = os.environ.get('INTERNAL_API_SECRET', 'zhamlik-internal-secret-2024')

# Helper function для проксирования запросов с аутентификацией
def proxy_to_zhamlik(endpoint, params=None, method='GET', json_data=None):
    """
    Проксирует запрос к основному API Zhamlik с передачей сессии.
    """
    url = f"{ZHAMLIK_API_URL}{endpoint}"

    # Получаем cookies из запроса
    cookies = {}
    if 'session' in request.cookies:
        cookies['session'] = request.cookies.get('session')

    try:
        if method == 'GET':
            response = requests.get(url, params=params, cookies=cookies, timeout=10)
        elif method == 'POST':
            response = requests.post(url, params=params, json=json_data, cookies=cookies, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, params=params, cookies=cookies, timeout=10)

        # Проверяем Content-Type ответа
        content_type = response.headers.get('Content-Type', '')

        if 'application/json' not in content_type:
            # API вернул HTML вместо JSON (например, страницу логина)
            if response.status_code in [301, 302, 307, 308]:
                # Редирект на страницу логина
                return {'error': 'unauthorized', 'message': 'Требуется авторизация'}
            else:
                return {'error': 'api_error', 'message': f'API вернул HTML вместо JSON (код {response.status_code}). Требуется авторизация.'}

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            return {'error': 'unauthorized', 'message': 'Требуется авторизация'}
        else:
            return {'error': 'api_error', 'message': f'API вернул код {response.status_code}'}

    except requests.exceptions.Timeout:
        return {'error': 'timeout', 'message': 'Превышено время ожидания'}
    except requests.exceptions.ConnectionError:
        return {'error': 'connection_error', 'message': 'Не удалось подключиться к API Zhamlik'}
    except ValueError as e:
        # Ошибка парсинга JSON
        return {'error': 'json_error', 'message': f'Ошибка парсинга JSON: {str(e)}'}
    except Exception as e:
        return {'error': 'unknown', 'message': str(e)}


def proxy_to_internal_zhamlik(endpoint, params=None, method='GET', json_data=None):
    """
    Проксирует запрос к internal API Zhamlik с shared secret авторизацией.
    Используется для endpoints которые не требуют Flask-Login сессии.
    """
    url = f"{ZHAMLIK_API_URL}{endpoint}"

    # Добавляем shared secret как header
    headers = {
        'X-Internal-Auth': INTERNAL_API_SECRET
    }

    try:
        if method == 'GET':
            response = requests.get(url, params=params, headers=headers, timeout=10)
        elif method == 'POST':
            response = requests.post(url, params=params, json=json_data, headers=headers, timeout=120)
        elif method == 'DELETE':
            response = requests.delete(url, params=params, headers=headers, timeout=10)

        # Проверяем Content-Type ответа
        content_type = response.headers.get('Content-Type', '')

        if 'application/json' not in content_type:
            return {'error': 'api_error', 'message': f'API вернул HTML вместо JSON (код {response.status_code})'}

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            return {'error': 'unauthorized', 'message': 'Internal API authorization failed'}
        else:
            return {'error': 'api_error', 'message': f'API вернул код {response.status_code}'}

    except requests.exceptions.Timeout:
        return {'error': 'timeout', 'message': 'Превышено время ожидания'}
    except requests.exceptions.ConnectionError:
        return {'error': 'connection_error', 'message': 'Не удалось подключиться к API Zhamlik'}
    except ValueError as e:
        return {'error': 'json_error', 'message': f'Ошибка парсинга JSON: {str(e)}'}
    except Exception as e:
        return {'error': 'unknown', 'message': str(e)}

# Crypto API Routes - прокси к основному Zhamlik

@app.route('/api/crypto/platforms')
def crypto_platforms():
    """Список крипто-бирж"""
    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/platforms')

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/platforms')

    if 'error' in data:
        return jsonify({'platforms': [], 'error': data.get('message')})

    return jsonify(data)

@app.route('/api/crypto/platforms', methods=['POST'])
def crypto_create_platform():
    """Создание новой биржи"""
    data_dict = request.get_json()

    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/platforms', method='POST', json_data=data_dict)

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/platforms', method='POST', json_data=data_dict)

    if 'error' in data:
        return jsonify({'success': False, 'message': data.get('message', 'Ошибка создания')}), 400

    return jsonify(data)

@app.route('/api/crypto/platforms/<int:platform_id>/sync', methods=['POST'])
def crypto_sync_platform(platform_id):
    """Синхронизация биржи"""
    # Сначала пробуем regular API
    data = proxy_to_zhamlik(f'/api/mobile/crypto/platforms/{platform_id}/sync', method='POST')

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik(f'/api/internal/crypto/platforms/{platform_id}/sync', method='POST')

    if 'error' in data:
        return jsonify({'success': False, 'message': data.get('message', 'Ошибка синхронизации')}), 400

    return jsonify(data)

@app.route('/api/crypto/platforms/<int:platform_id>')
def crypto_platform_detail(platform_id):
    """Детали биржи с активами"""
    # Сначала пробуем regular API
    data = proxy_to_zhamlik(f'/api/mobile/crypto/platforms/{platform_id}')

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik(f'/api/internal/crypto/platforms/{platform_id}')

    if 'error' in data:
        return jsonify({'error': data.get('message')}), 400

    return jsonify(data)

@app.route('/api/crypto/assets', methods=['POST'])
def crypto_create_asset():
    """Ручное добавление актива"""
    data_dict = request.get_json()

    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/assets', method='POST', json_data=data_dict)

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/assets', method='POST', json_data=data_dict)

    if 'error' in data:
        return jsonify({'success': False, 'message': data.get('message', 'Ошибка добавления')}), 400

    return jsonify(data)

@app.route('/api/crypto/overview')
def crypto_overview():
    """Главный экран с крипто-портфелем и последними транзакциями"""
    # Сначала пробуем regular API с сессией
    data = proxy_to_zhamlik('/api/mobile/crypto/overview')

    # Если любая ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/overview')

        # Если все еще ошибка
        if 'error' in data:
            return jsonify({
                'total_balance_usd': 0,
                'total_balance_rub': 0,
                'assets': [],
                'recent_transactions': [],
                'error': data.get('message', 'Не удалось загрузить данные')
            })

    return jsonify(data)

@app.route('/api/crypto/assets')
def crypto_assets():
    """Список всех крипто-активов"""
    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/assets')

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/assets')

    if 'error' in data:
        return jsonify({'assets': [], 'error': data.get('message')})

    return jsonify(data)

@app.route('/api/crypto/transactions')
def crypto_transactions():
    """Список крипто-транзакций с фильтрами"""
    params = {
        'page': request.args.get('page', 1),
        'filter_type': request.args.get('filter_type', 'all')
    }

    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/transactions', params)

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/transactions', params)

    if 'error' in data:
        return jsonify({
            'transactions': [],
            'has_next': False,
            'total': 0,
            'error': data.get('message')
        })

    return jsonify(data)

@app.route('/api/crypto/analytics')
def crypto_analytics():
    """Аналитика по крипто-портфелю"""
    params = {
        'days': request.args.get('days', 30)
    }

    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/analytics', params)

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/analytics', params)

    if 'error' in data:
        return jsonify({
            'portfolio_distribution': [],
            'performance_chart': [],
            'error': data.get('message')
        })

    return jsonify(data)

@app.route('/api/crypto/prices')
def crypto_prices():
    """Текущие цены криптовалют"""
    # Сначала пробуем regular API
    data = proxy_to_zhamlik('/api/mobile/crypto/prices')

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik('/api/internal/crypto/prices')

    if 'error' in data:
        return jsonify({'prices': [], 'error': data.get('message')})

    return jsonify(data)

@app.route('/api/crypto/platforms/<int:platform_id>', methods=['DELETE'])
def crypto_delete_platform(platform_id):
    """Удаление биржи"""
    # Сначала пробуем regular API
    data = proxy_to_zhamlik(f'/api/mobile/crypto/platforms/{platform_id}', method='DELETE')

    # Если ошибка, используем internal API
    if 'error' in data:
        data = proxy_to_internal_zhamlik(f'/api/internal/crypto/platforms/{platform_id}', method='DELETE')

    if 'error' in data:
        return jsonify({'success': False, 'message': data.get('message', 'Ошибка удаления')}), 400

    return jsonify(data)

# Static files
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Main routes
@app.route('/')
def index():
    return render_template('index.html')

# Health check endpoint
@app.route('/health')
def health():
    """Проверка здоровья приложения и подключения к API"""
    try:
        # Try accessing the crypto overview endpoint
        response = requests.get(f"{ZHAMLIK_API_URL}/api/mobile/crypto/overview", timeout=5)
        api_status = 'ok' if response.status_code in [200, 401] else 'error'
    except:
        api_status = 'unreachable'

    return jsonify({
        'status': 'ok',
        'zhamlik_api': api_status,
        'version': '1.0.0'
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)
