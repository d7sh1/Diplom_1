import psycopg2  # Импортируем библиотеку для работы с PostgreSQL.
import json  # Импортируем стандартную библиотеку для работы с JSON.

# Функция для проверки имен таблиц
def check_table_naming(conn):
    cursor = conn.cursor()  # Создаем курсор для выполнения запросов к базе данных.
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    tables = cursor.fetchall()  # Получаем все таблицы из результата запроса.
    errors = []  # Список для хранения ошибок.
    
    for table in tables:  # Перебираем все полученные имена таблиц.
        table_name = table[0]  # Извлекаем имя таблицы из результата.
        
        # Проверяем, что имя таблицы:
        # 1. В нижнем регистре.
        # 2. Содержит только символы ASCII.
        if not table_name.islower() or not table_name.isascii():
            errors.append(f"Invalid table name: {table_name}")  # Если условие не выполнено, добавляем ошибку в список.
    
    cursor.close()  # Закрываем курсор после выполнения запроса.
    return errors  # Возвращаем список ошибок.

# Функция для проверки имен столбцов
def check_column_naming(conn):
    cursor = conn.cursor()  # Создаем курсор для выполнения запросов.
    cursor.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public';
    """)
    columns = cursor.fetchall()  # Получаем все столбцы из результата запроса.
    errors = []  # Список для хранения ошибок.

    for table, column in columns:  # Перебираем все столбцы.
        # Проверяем, что имя столбца:
        # 1. В нижнем регистре.
        # 2. Содержит только символы ASCII.
        if not column.islower() or not column.isascii():
            errors.append(f"Invalid column name: {table}.{column}")  # Если условие не выполнено, добавляем ошибку.

    cursor.close()  # Закрываем курсор.
    return errors  # Возвращаем список ошибок.

# Функция для проверки ограничений (первичные и внешние ключи)
def check_constraints(conn):
    cursor = conn.cursor()  # Создаем курсор для выполнения запросов.
    cursor.execute("""
        SELECT constraint_name, table_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_schema = 'public';
    """)
    constraints = cursor.fetchall()  # Получаем все ограничения из результата запроса.
    errors = []  # Список для хранения ошибок.

    for constraint_name, table, constraint in constraints:  # Перебираем все ограничения.
        if constraint == 'PRIMARY KEY':  # Если это первичный ключ.
            if not constraint_name.lower().startswith('pk_'):  # Проверяем имя ограничения на префикс 'pk_'.
                errors.append(f"Primary key constraint without 'pk_' prefix: {constraint_name}")  # Добавляем ошибку.

        elif constraint == 'FOREIGN KEY':  # Если это внешний ключ.
            if not constraint_name.lower().startswith('fk_'):  # Проверяем имя ограничения на префикс 'fk_'.
                errors.append(f"Foreign key constraint without 'fk_' prefix: {constraint_name}")  # Добавляем ошибку.

    cursor.close()  # Закрываем курсор.
    return errors  # Возвращаем список ошибок.
        
# Функция для проверки количества строк в таблицах
def check_row_counts(conn):
    cursor = conn.cursor()  # Создаем курсор.
    cursor.execute(""" 
        SELECT relname, n_live_tup
        FROM pg_stat_user_tables
        WHERE schemaname = 'public';
    """)
    row_counts = cursor.fetchall()  # Получаем количество строк для каждой таблицы.
    errors = []  # Список для хранения ошибок.

    for table, count in row_counts:  # Перебираем все таблицы и их количество строк.
        if count == 0:  # Если таблица пуста (количество строк = 0).
            errors.append(f"Table {table} is empty")  # Добавляем ошибку, если таблица пуста.

    cursor.close()  # Закрываем курсор.
    return errors  # Возвращаем список ошибок.

# Основная функция для выполнения всех проверок
def validate_database():
    # Устанавливаем соединение с базой данных PostgreSQL.
    conn = psycopg2.connect(
            dbname="university",  # Имя базы данных.
            user="postgres",  # Имя пользователя базы данных.
            password="qwerty",  # Пароль.
            host="localhost",  # Адрес хоста (локально, на машине).
            port=5432,  # Порт, на котором работает PostgreSQL.
            client_encoding="UTF8"  # Кодировка клиента.
        )
    
    errors = []  # Список для хранения всех ошибок.
    
    # Вызываем функции проверки и добавляем их результаты (ошибки) в общий список.
    errors.extend(check_table_naming(conn))
    errors.extend(check_column_naming(conn))
    errors.extend(check_constraints(conn))
    errors.extend(check_row_counts(conn))
    
    conn.close()  # Закрываем соединение с базой данных.
    return errors  # Возвращаем все ошибки, которые были найдены.

# Основная точка входа в программу
if __name__ == "__main__":  # Если этот скрипт выполняется как основной (не импортирован).
    validation_errors = validate_database()  # Выполняем все проверки базы данных.

    if validation_errors:  # Если есть ошибки (список ошибок не пуст).
        # Открываем файл 'errors.json' для записи ошибок.
        with open('errors.json', 'w') as f:
            json.dump(validation_errors, f, indent=4)  # Записываем ошибки в формате JSON в файл с отступами.

        # Выводим сообщение, что ошибки найдены.
        print("Найдены ошибки, они записаны в 'errors.json'")
    else:
        # Если ошибок нет, выводим сообщение.
        print("Нет ошибок")