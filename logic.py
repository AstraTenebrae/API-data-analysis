import pandas as pd
import os, shutil
from typing import Literal
from flask import render_template
from sqlalchemy import text, inspect

import base64
import matplotlib.pyplot
from matplotlib.figure import Figure
from io import BytesIO


def data_analysis_means(data: pd.DataFrame, dec=3, num_only=True) -> pd.Series:
    '''
    Среднее значение для каждого столбца
    '''
    means = data.mean(numeric_only=num_only).round(dec)
    return means

def data_analysis_medians(data: pd.DataFrame, dec=3, num_only=True) -> pd.Series:
    '''
    Медиана для каждого столбца
    '''
    medians = data.median(numeric_only=num_only).round(dec)
    return medians

def data_analysis_correlation(data: pd.DataFrame, dec=3, num_only=True) -> pd.DataFrame:
    '''
    Корреляционная матрица
    '''
    correlation_matrix = data.corr(numeric_only=num_only).round(dec)
    return correlation_matrix

def delete_one_file(file_name, folder):
    try:
        file_path = os.path.join(folder, file_name)
        if not os.path.exists(file_path):
            return f"Указанный файл {file_name} уже отсутствует на сервере", 200
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                return f"Файл удалён: {file_name}", 200
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
                return f"Папка удалена: {file_name}", 200
            else:
                return f"Система не может считает указанный объект ни за файл, ни за папку", 200
        except Exception as e:
            return f'Ошибка при удалении файла {file_name}: {str(e)}', 500
    except Exception as e:
        return f"Ошибка при удалении файла {file_name}: {str(e)}", 500

def delete_all(folder):
    try:
        for file_name in os.listdir(folder):
            file_path = os.path.join(folder, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                return f'Ошибка при удалении файла {file_name}: {str(e)}', 500
        
        return 'Папка uploads успешно очищена', 200
    
    except Exception as e:
        return f'Ошибка при очистке папки: {str(e)}', 500
    

def upload_to_db_one_file(data: pd.DataFrame, table_name: str, if_exists: Literal['fail', 'replace', 'append'] ='replace', engine=None) -> tuple:
    try:
        if engine is None:
            return "Не указано подключение к БД", 500
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False
        )
        return None, 200
    
    except Exception as e:
        return f"Ошибка при загрузке в БД: {str(e)}", 500
    
def upload_to_directory(uploaded_files, app, *args, **kwargs):
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])
    try:
        for file in uploaded_files:
            file_name = file.filename
            if not file_name:
                return "Имя файла отсутствует", 400
            
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
            file.save(file_path)
        
    except Exception as e:
        return f"Ошибка при сохранении файла в файловую систему: {str(e)}", 500
    return f"Файлы успешно сохранены в файловую систему", 201


def upload_to_db(uploaded_files, app, engine, *args, **kwargs) -> tuple:
    try:
        counter = 0
        for file in uploaded_files:
            file_name = file.filename
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
            file.save(file_path)
            file_extension = file_name.split('.')[-1].lower()
            if file_extension == 'csv':
                data = pd.read_csv(file_path, encoding="utf-8")
            elif file_extension in ['xls', 'xlsx']:
                data = pd.read_excel(file_path)
            else:
                delete_one_file(file_name=file_name, folder=app.config['UPLOAD_FOLDER'])
                return f"Обработка файлов с данным расширением ещё не реализована: {file_extension}. Загрузка файлов в базу данных остановлена. Загружено: {counter} файлов.", 415
            delete_one_file(file_name=file_name, folder=app.config['UPLOAD_FOLDER'])
            clean_file_name = file_name.rsplit('.', 1)[0]
            result = upload_to_db_one_file(data=data, table_name=clean_file_name, engine=engine)
            if result[1] != 200:
                return result
            counter += 1
    except Exception as e:
        return f"Ошибка при сохранении файла в базу данных: {str(e)}", 500
    else:
        return "Файлы успешно загружены в базу данных", 200

def delete_from_directory(file_name, app, *args, **kwargs):
    folder = app.config['UPLOAD_FOLDER']

    if not os.path.exists(folder):
        return "Папка uploads не существует", 200
    
    if not (file_name is None):
        return delete_one_file(file_name=file_name, folder=folder)

    return delete_all(folder=folder)

def delete_from_db(file_name, app, engine, *args, **kwargs):
    try:
        if not file_name:
            return "Имя файла не указано", 400
        table_name = file_name.rsplit('.', 1)[0]
        if not inspect(engine).has_table(table_name):
            return f"Таблица {table_name} не найдена в базе данных", 404
        with engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))        
        return f"Таблица {table_name} успешно удалена из базы данных", 200
    except Exception as e:
        return f"Ошибка при удалении таблицы из базы данных: {str(e)}", 500

def data_stats_directory(file_name, app, *args, **kwargs):
    file = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
    if not os.path.exists(file):
        return "файл не был найден: возможно, было указано неверное расширение или в имени есть ошибка", 415
    file_extension = file.split('.')[-1].lower()
    try:
        if file_extension == 'csv':
            data = pd.read_csv(file, encoding="utf-8")
        elif file_extension in ['xls', 'xlsx']:
            data = pd.read_excel(file)
        else:
            return "Обработка файлов с данным расширением ещё не реализована", 415
    except Exception as e:
        return f"Ошибка при чтении файла: {str(e)}", 400
    return render_template("stats.html", stats_means=data_analysis_means(data), stats_medians=data_analysis_medians(data), stats_correlation=data_analysis_correlation(data)), 200

def data_stats_db(file_name, app, engine, *args, **kwargs):
    table_name = file_name.rsplit('.', 1)[0]
    if not inspect(engine).has_table(table_name):
        return f"Таблица {table_name} не найдена в базе данных", 404
    data = pd.read_sql_table(table_name, engine)    
    return render_template("stats.html", stats_means=data_analysis_means(data), stats_medians=data_analysis_medians(data), stats_correlation=data_analysis_correlation(data)), 200

def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    data_copy = data.copy()
    data_no_duplicates = data_copy.drop_duplicates()
    
    for column in data_no_duplicates.columns:
        if data_no_duplicates[column].dtype in ['int64', 'float64']:
            if data_no_duplicates[column].isna().any():
                data_no_duplicates[column] = data_no_duplicates[column].fillna(data_no_duplicates[column].median())

        elif data_no_duplicates[column].dtype == 'object':
            if data_no_duplicates[column].isna().any():
                mode_value = data_no_duplicates[column].mode()
                if not mode_value.empty:
                    data_no_duplicates[column] = data_no_duplicates[column].fillna(mode_value[0])
                else:
                    data_no_duplicates[column] = data_no_duplicates[column].fillna('')
    
    return data_no_duplicates

def clean_file_data(file_name, app, *args, **kwargs):
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
    if not os.path.exists(file_path):
        return "Файл не был найден: возможно, было указано неверное расширение или в имени есть ошибка", 415

    file_extension = file_name.split('.')[-1].lower()
    try:
        if file_extension == 'csv':
            data = pd.read_csv(file_path, encoding="utf-8")
            cleaned_data = clean_data(data)
            cleaned_data.to_csv(file_path, index=False, encoding="utf-8")
        elif file_extension in ['xls', 'xlsx']:
            data = pd.read_excel(file_path)
            cleaned_data = clean_data(data)
            cleaned_data.to_excel(file_path, index=False)
        else:
            return "Обработка файлов с данным расширением ещё не реализована", 415
        return f"Данные файла {file_name} успешно очищены", 200
    except Exception as e:
        return f"Ошибка при очистке данных файла: {str(e)}", 500
    
def clean_db_data(file_name, app, engine, *args, **kwargs):
    try:
        table_name = file_name.rsplit('.', 1)[0]
        if not inspect(engine).has_table(table_name):
            return f"Таблица {table_name} не найдена в базе данных", 404
        data = pd.read_sql_table(table_name, engine)
        cleaned_data = clean_data(data)
        cleaned_data.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False
        )
        return f"Данные таблицы {table_name} успешно очищены", 200
    except Exception as e:
        return f"Ошибка при очистке данных в базе данных: {str(e)}", 500
    

def create_plots(data: pd.DataFrame, x_column: str, dec=3) -> tuple:
    if x_column not in data.columns:
        return f"Столбец {x_column} не был найден", 400
    if not pd.api.types.is_numeric_dtype(data[x_column]):
        return f"Столбец {x_column} не является числовым", 400
    
    plots = {}
    numeric_columns = data.select_dtypes(include=['int64', 'float64']).columns

    for y_column in numeric_columns:
        if y_column == x_column:
            continue
        else:
            fig = Figure(figsize=(12, 8))
            ax = fig.add_subplot(111)
            ax.scatter(data[x_column], data[y_column], color="red")
            ax.set_xlabel(x_column, fontsize=12)
            ax.set_ylabel(y_column, fontsize=12)
            ax.set_title(f"{y_column} / {x_column}", fontsize=16)
            ax.grid(visible=True)
            
            buf = BytesIO()
            fig.savefig(buf, format="png", bbox_inches='tight')
            buf.seek(0)

            image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
            plots[y_column] = image_base64

            matplotlib.pyplot.close(fig)

    return plots, 200

def show_plots_file(file_name, x_column, app, *args, **kwargs):
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
    if not os.path.exists(file_path):
        return "Файл не был найден: возможно, было указано неверное расширение или в имени есть ошибка", 415
    
    file_extension = file_name.split('.')[-1].lower()
    try:
        if file_extension == 'csv':
            data = pd.read_csv(file_path, encoding="utf-8")
        elif file_extension in ['xls', 'xlsx']:
            data = pd.read_excel(file_path)
        else:
            return "Обработка файлов с данным расширением ещё не реализована", 415

        plots, code = create_plots(data, x_column)
        if code == 200:
            return render_template("plots.html", plots=plots, x_column=x_column), 200
        else:
            return plots, code        
    except ValueError as e:
        return f"Ошибка в данных: {str(e)}", 400
    except Exception as e:
        return f"Ошибка при построении графиков: {str(e)}", 500
    
def show_plots_db(file_name, x_column, app, engine, *args, **kwargs):
    table_name = file_name.rsplit('.', 1)[0]
    try:
        if not inspect(engine).has_table(table_name):
            return f"Таблица {table_name} не найдена в базе данных", 404
        
        data = pd.read_sql_table(table_name, engine)
        plots, code = create_plots(data, x_column)
        if code == 200:
            return render_template("plots.html", plots=plots, x_column=x_column), 200
        else:
            return plots, code
        
    except ValueError as e:
        return f"Ошибка в данных: {str(e)}", 400
    except Exception as e:
        return f"Ошибка при построении графиков: {str(e)}", 500