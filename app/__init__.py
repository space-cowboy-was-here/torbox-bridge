from flask import Flask

app = Flask(__name__)

# Import routes
from .main import *

# Additional application setup can be added here if needed.