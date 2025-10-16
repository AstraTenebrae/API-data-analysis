
DATA_ENDPOINT = '/data/'
UPLOAD_FOLDER = 'uploads'

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'db_name': 'data_analysis',
    'user': 'username',
    'password': 'password',
}
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db_name']}"

allowed_file_extensions = ['csv','xls','xlsx']