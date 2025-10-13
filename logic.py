from pandas import Series, DataFrame
import os, shutil

def data_analysis_means(data: DataFrame, dec=3, num_only=True) -> Series:
    '''
    Среднее значение для каждого столбца
    '''
    means = data.mean(numeric_only=num_only).round(dec)
    return means

def data_analysis_medians(data: DataFrame, dec=3, num_only=True) -> Series:
    '''
    Медиана для каждого столбца
    '''
    medians = data.median(numeric_only=num_only).round(dec)
    return medians

def data_analysis_correlation(data: DataFrame, dec=3, num_only=True) -> DataFrame:
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