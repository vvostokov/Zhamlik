import os
from flask import Flask
from decimal import Decimal
from datetime import datetime, timezone
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

from extensions import db, migrate, scheduler, login_manager, mail

def create_app():
    """Application Factory."""
    # Загружаем переменные окружения из файла .env в самом начале
    load_dotenv()
    
    app = Flask(__name__)
    basedir = os.path.abspath(os.path.dirname(__file__))

    # --- Configuration ---
    def generate_fernet_key(secret_key: str, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC( # Consider using a constant salt or generating one per user and storing it securely
            algorithm=hashes.SHA256(),  
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
        return key
    app.config['FERNET_KEY'] = os.environ.get('FERNET_KEY') or Fernet.generate_key().decode() # Ensure a Fernet key exists
    app.config['SECRET_KEY'] = app.config['FERNET_KEY']
    app.config['SQLALCHEMY_DATABASE_URI'] = (
        os.environ.get('DATABASE_URL') or
        'sqlite:///' + os.path.join(basedir, 'app.db')
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['UPLOAD_FOLDER'] = os.path.join(basedir, 'uploads')
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.config['ITEMS_PER_PAGE'] = 20
    app.config['FNS_API_USERNAME'] = os.environ.get('FNS_API_USERNAME') # Ваш ИНН
    app.config['FNS_API_PASSWORD'] = os.environ.get('FNS_API_PASSWORD')
    # --- CryptoCompare News API Key ---
    app.config['CRYPTOCOMPARE_API_KEY'] = os.environ.get('CRYPTOCOMPARE_API_KEY')

    # --- Email Configuration ---
    app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER', 'smtp.gmail.com')
    app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', '587'))
    app.config['MAIL_USE_TLS'] = os.environ.get('MAIL_USE_TLS', 'True').lower() == 'true'
    app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
    app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
    app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_DEFAULT_SENDER', 'noreply@zhamlik.local')

    # --- Scheduler Configuration ---
    # Use single worker to avoid APScheduler multi-process issues
    app.config['SCHEDULER_API_ENABLED'] = False  # Disable API for security

    # Force single process execution
    app.config['SCHEDULER_JOB_DEFAULTS'] = {
        'coalesce': True,
        'max_instances': 1
    }

    # --- Initialize Extensions ---
    db.init_app(app)
    migrate.init_app(app, db)
    scheduler.init_app(app)
    login_manager.init_app(app)
    mail.init_app(app)

    # Initialize background jobs with proper app context
    from background_tasks import init_background_jobs
    init_background_jobs(scheduler, app)
    scheduler.start()

    # --- Register Jinja Filters ---
    @app.template_filter()
    def trim_zeros(value):
        if isinstance(value, Decimal):
            # Format as a standard decimal string without scientific notation
            value = "{:f}".format(value)
        if isinstance(value, str) and '.' in value:
            return value.rstrip('0').rstrip('.')
        return value

    @app.template_filter()
    def money_format(value, precision=2):
        """Форматирует число как денежную сумму с пробелами в качестве разделителей тысяч."""
        if value is None:
            return '-'
        try:
            return f"{Decimal(value):,.{precision}f}".replace(',', ' ')
        except (ValueError, TypeError):
            return str(value)

    @app.template_filter()
    def timestamp_to_datetime(ts):
        """Converts a UNIX timestamp to a timezone-aware datetime object."""
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except (ValueError, TypeError):
            return None

    @app.template_filter()
    def datetime_format(dt, fmt='%d.%m.%Y %H:%M'):
        """Formats a datetime object into a string."""
        return dt.strftime(fmt) if dt else ''

    # --- Register Blueprints ---
    with app.app_context():
        # Import blueprints inside the context
        from routes import main_bp
        from routes.auth import auth_bp
        from api_routes import api_bp
        from commands import analytics_cli, seed_cli
        from securities_logic import securities_bp
        from routes.notifications import notifications_bp
        from mobile_api import mobile_bp
        from versions_api import versions_bp

        app.register_blueprint(main_bp)
        app.register_blueprint(auth_bp)
        app.register_blueprint(securities_bp)
        app.register_blueprint(api_bp, url_prefix='/api')
        app.register_blueprint(mobile_bp)
        app.register_blueprint(notifications_bp)
        app.register_blueprint(versions_bp)
        app.cli.add_command(analytics_cli)
        app.cli.add_command(seed_cli)

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5001)
