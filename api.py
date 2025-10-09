import os
from flask import Flask, render_template, request


UPLOAD_FOLDER = 'uploads'
os.makedirs('uploads', exist_ok=True)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route("/", methods=['GET'])
def show_upload_page():
    return render_template("upload.html")

@app.route("/upload", methods=['POST'])
def upload_files():
    uploaded_files = request.files.getlist("file_name")
    for file_i in uploaded_files:
        file_i_name = file_i.filename
        if file_i_name:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_i_name)
            file_i.save(file_path)
    return "файлы отправлены на сервер", 201