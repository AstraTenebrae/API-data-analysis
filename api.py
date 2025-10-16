import os

from sqlalchemy import create_engine
from flask import Flask, render_template, request
from logic import upload_to_directory, upload_to_db, delete_from_directory, delete_from_db, data_stats_directory, data_stats_db, clean_file_data, clean_db_data
from settings import UPLOAD_FOLDER, DATA_ENDPOINT, DATABASE_URL, allowed_file_extensions

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

engine = create_engine(DATABASE_URL)


@app.route("/", methods=['GET'])
def show_upload_page():
    return render_template("upload.html", allowed_extensions='.'+',.'.join(allowed_file_extensions))


@app.route("/upload", methods=['POST'])
def upload_files() -> tuple:
    uploaded_files = request.files.getlist("file_name")
    if uploaded_files is None:
        return "Файлы не указаны", 400
#    return upload_to_directory(uploaded_files=uploaded_files, app=app)
    return upload_to_db(uploaded_files=uploaded_files, app=app, engine=engine)

 
@app.route(DATA_ENDPOINT+'stats', methods=['GET'])
def data_stats():
    file_name = request.args.get(key='filename', default=None)
    if file_name is None:
        return "Имя файла не указано", 400
#    return data_stats_directory(file_name=file_name, app=app)
    return data_stats_db(file_name=file_name, app=app, engine=engine)
    

@app.route(DATA_ENDPOINT+'delete', methods=['GET'])
def data_delete() -> tuple:
    file_name = request.args.get(key='filename', default=None)
    if file_name is None:
        return "Имя файла не указано", 400
#    return delete_from_directory(file_name=file_name, app=app)
    return delete_from_db(file_name=file_name, app=app, engine=engine)


@app.route(DATA_ENDPOINT+'clean', methods=['GET'])
def data_clean() -> tuple:
    file_name = request.args.get(key='filename', default=None)
    if file_name is None:
        return "Имя файла не указано", 400
#    return clean_file_data(file_name=file_name, app=app)
    return clean_db_data(file_name=file_name, app=app, engine=engine)