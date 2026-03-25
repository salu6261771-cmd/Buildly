import os
from datetime import datetime

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
AWS_S3_BUCKET = os.getenv('S3_BUCKET_NAME', '')
AWS_REGION = 'us-east-1'

# Date-based partitioning
CURRENT_DATE = datetime.now()
YEAR = CURRENT_DATE.strftime('%Y')
MONTH = CURRENT_DATE.strftime('%m')
DAY = CURRENT_DATE.strftime('%d')

# S3 Paths
S3_BASE_PATH = 'boutiqaat-data'
S3_IMAGES_PATH = f'{S3_BASE_PATH}/year={YEAR}/month={MONTH}/day={DAY}/women-makeup/images'
S3_EXCEL_PATH = f'{S3_BASE_PATH}/year={YEAR}/month={MONTH}/day={DAY}/women-makeup'

# Website Configuration
BASE_URL = 'https://www.boutiqaat.com'
CATEGORY_URL = f'{BASE_URL}/ar-kw/women/makeup/c/'
MAIN_CATEGORY = 'makeup'

# Timeout and retry settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# Image settings
IMAGE_QUALITY = 80
MAX_IMAGE_SIZE = (400, 400)

# Local temporary directory
TEMP_DIR = './temp_downloads'

# Excel settings
EXCEL_DATE_STR = CURRENT_DATE.strftime('%Y-%m-%d')
