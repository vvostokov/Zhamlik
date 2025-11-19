from flask import Blueprint

main_bp = Blueprint('main', __name__)

# Import routes to register them with the blueprint
# (This will be done after the files are created to avoid circular imports immediately, 
#  but generally we import the modules here so that when 'main_bp' is imported in app.py, 
#  all routes are registered)
from . import banking, investments, debts, analytics, general