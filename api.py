import os, shutil
import glob
import pandas as pd

from flask import Flask, render_template, request
from logic import data_analysis_means, data_analysis_medians, data_analysis_correlation, delete_one_file, delete_all

DATA_ENDPOINT = '/data/'

UPLOAD_FOLDER = 'uploads'
os.makedirs('uploads', exist_ok=True)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

allowed_file_extensions = ['csv','xls','xlsx']

@app.route("/", methods=['GET'])
def show_upload_page():
    return render_template("upload.html", allowed_extensions='.'+',.'.join(allowed_file_extensions))


@app.route("/upload", methods=['POST'])
def upload_files():
    uploaded_files = request.files.getlist("file_name")
    for file_i in uploaded_files:
        file_i_name = file_i.filename
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs('uploads')
        if file_i_name:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_i_name)
            file_i.save(file_path)
    return "файлы отправлены на сервер", 201


@app.route(DATA_ENDPOINT+'stats', methods=['GET'])
def data_stats():
    file_name = request.args.get(key='filename', default=None)
    if file_name is None:
        return "файл не указан", 400

    file = os.path.join(app.config['UPLOAD_FOLDER'], file_name)

    if not os.path.exists(file):
        return "файл не был найден: возможно, было указано неверное расширение или в имени есть ошибка", 415
    file_extension = file.split('.')[-1].lower()

    try:
        if file_extension == 'csv':
            data = pd.read_csv(file)
        elif file_extension in ['xls', 'xlsx']:
            data = pd.read_excel(file)
        else:
            return "обработка файлов с данным расширением разрешена, но ещё не реализована", 415
    except Exception as e:
        return f"Ошибка при чтении файла: {str(e)}", 400
    
    return render_template("stats.html", stats_means=data_analysis_means(data), stats_medians=data_analysis_medians(data), stats_correlation=data_analysis_correlation(data))


@app.route(DATA_ENDPOINT+'clean', methods=['GET'])
def data_clean() -> tuple:
    file_name = request.args.get(key='filename', default=None)
    folder = app.config['UPLOAD_FOLDER']

    if not os.path.exists(folder):
        return "Папка uploads не существует", 200
    
    if not (file_name is None):
        return delete_one_file(file_name=file_name, folder=folder)

    return delete_all(folder=folder)