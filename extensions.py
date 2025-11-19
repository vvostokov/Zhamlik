from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_apscheduler import APScheduler
from flask_login import LoginManager

db = SQLAlchemy()
migrate = Migrate()
scheduler = APScheduler()
login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Пожалуйста, войдите, чтобы получить доступ к этой странице.'
