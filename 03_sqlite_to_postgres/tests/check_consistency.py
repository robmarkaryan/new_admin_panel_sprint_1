import psycopg2
import sqlite3 as sql
import dateutil.parser

dsn_postgre = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

dsn_sqlite = {
    'path_to_db': r'C:\Users\robma\PycharmProjects\admin_panel\03_sqlite_to_postgres\db.sqlite'
}


def convert_timeobj(val):
    val = val.decode('utf8')
    return dateutil.parser.parse(val)


def check_count(sqlite_cursor, postgre_cursor):
    """
    Проверка таблиц на соответствие по колву

    :param sqlite_cursor: Курсор к бд sqlite
    :type sqlite_cursor: cursor
    :param postgre_cursor: Курсор к бд postgre
    :type postgre_cursor: cursor
    """

    sqlite_cursor.execute("SELECT * FROM person")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute("SELECT * FROM content.person")
    postgre_person_data = postgre_cursor.fetchall()

    if len(sqlite_person_data) == len(postgre_person_data):
        print('Таблицы person идентичны по колву')
    else:
        raise 'Таблицы person не идентичны по колву'

    sqlite_cursor.execute("SELECT * FROM film_work")
    sqlite_film_work_data = sqlite_cursor.fetchall()



    sql.register_converter("timestamp", convert_timeobj)

    postgre_cursor.execute("SELECT * FROM content.film_work")
    postgre_film_work_data = postgre_cursor.fetchall()

    if len(sqlite_film_work_data) == len(postgre_film_work_data):
        print('Таблицы film_work идентичны по колву')
    else:
        raise 'Таблицы film_work не идентичны по колву'

    sqlite_cursor.execute("SELECT * FROM genre")
    sqlite_genre_data = sqlite_cursor.fetchall()

    postgre_cursor.execute("SELECT * FROM content.genre")
    postgre_genre_data = postgre_cursor.fetchall()

    if len(sqlite_genre_data) == len(postgre_genre_data):
        print('Таблицы genre идентичны по колву')
    else:
        raise 'Таблицы genre не идентичны по колву'

    sqlite_cursor.execute("SELECT * FROM genre_film_work")
    sqlite_genre_film_work_data = sqlite_cursor.fetchall()

    postgre_cursor.execute("SELECT * FROM content.genre_film_work")
    postgre_genre_film_work_data = postgre_cursor.fetchall()

    if len(sqlite_genre_film_work_data) == len(postgre_genre_film_work_data):
        print('Таблицы genre_film_work идентичны по колву')
    else:
        raise 'Таблицы genre_film_work не идентичны по колву'

    sqlite_cursor.execute("SELECT * FROM person_film_work")
    sqlite_person_film_work_data = sqlite_cursor.fetchall()

    postgre_cursor.execute("SELECT * FROM content.person_film_work")
    postgre_person_film_work_data = postgre_cursor.fetchall()

    if len(sqlite_person_film_work_data) == len(postgre_person_film_work_data):
        print('Таблицы person_film_work идентичны по колву')
    else:
        raise 'Таблицы person_film_work не идентичны по колву'


def check_values(sqlite_cursor, postgre_cursor):
    """
    Проверка таблиц на соответствие по значениям

    :param sqlite_cursor: Курсор к бд sqlite
    :type sqlite_cursor: cursor
    :param postgre_cursor: Курсор к бд postgre
    :type postgre_cursor: cursor
    """

    sqlite_cursor.execute("SELECT id, full_name, birth_date, created_at, updated_at FROM person")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute("SELECT id, full_name, birth_date, TO_CHAR(created,'YYYY-MM-DD HH24:MI:SS.FF6+00'), TO_CHAR(modified,'YYYY-MM-DD HH24:MI:SS.FF6+00') FROM content.person")
    postgre_person_data = postgre_cursor.fetchall()

    for sqlite_data in sqlite_person_data:
        find_element = False
        for postgre_data in postgre_person_data:
            if sqlite_data[0] == postgre_data[0] and sqlite_data[1] == postgre_data[1] \
                    and sqlite_data[2] == postgre_data[2] and sqlite_data[3][0:21] == postgre_data[3][0:21] \
                    and sqlite_data[4][0:21] == postgre_data[4][0:21]:
                find_element = True
        if not find_element:
            raise 'Списки не идентичны'

    sqlite_cursor.execute("SELECT id, name, description, created_at, updated_at FROM genre")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "SELECT id, name, description, TO_CHAR(created,'YYYY-MM-DD HH24:MI:SS.FF6+00'), TO_CHAR(modified,'YYYY-MM-DD HH24:MI:SS.FF6+00') FROM content.genre")
    postgre_person_data = postgre_cursor.fetchall()

    for sqlite_data in sqlite_person_data:
        find_element = False
        for postgre_data in postgre_person_data:
            if sqlite_data[0] == postgre_data[0] and sqlite_data[1] == postgre_data[1] \
                    and sqlite_data[2] == postgre_data[2] and sqlite_data[3][0:21] == postgre_data[3][0:21] \
                    and sqlite_data[4][0:21] == postgre_data[4][0:21]:
                find_element = True
        if not find_element:
            raise 'Списки не идентичны'

    sqlite_cursor.execute("SELECT id, title, description, creation_date, certificate, file_path, rating, type, created_at, updated_at FROM film_work")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute("SELECT id, title, description, creation_date, certificate, file_path, rating, type, TO_CHAR(created,'YYYY-MM-DD HH24:MI:SS.FF6+00'), TO_CHAR(modified,'YYYY-MM-DD HH24:MI:SS.FF6+00') FROM content.film_work")
    postgre_person_data = postgre_cursor.fetchall()

    for sqlite_data in sqlite_person_data:
        find_element = False
        for postgre_data in postgre_person_data:
            if sqlite_data[0] == postgre_data[0] and sqlite_data[1] == postgre_data[1] \
                    and sqlite_data[2] == postgre_data[2] and sqlite_data[3] == postgre_data[3] \
                    and sqlite_data[4] == postgre_data[4] and sqlite_data[5] == postgre_data[5] \
                    and sqlite_data[6] == postgre_data[6] and sqlite_data[7] == postgre_data[7] \
                    and sqlite_data[8][0:21] == postgre_data[8][0:21] \
                    and sqlite_data[9][0:21] == postgre_data[9][0:21]:
                find_element = True
        if not find_element:
            raise 'Списки не идентичны'

    sqlite_cursor.execute("SELECT id, film_work_id, genre_id, created_at FROM genre_film_work")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "SELECT id, film_work_id, genre_id, TO_CHAR(created,'YYYY-MM-DD HH24:MI:SS.FF6+00') FROM content.genre_film_work")
    postgre_person_data = postgre_cursor.fetchall()

    for sqlite_data in sqlite_person_data:
        find_element = False
        for postgre_data in postgre_person_data:
            if sqlite_data[0] == postgre_data[0] and sqlite_data[1] == postgre_data[1] \
                    and sqlite_data[2] == postgre_data[2] and sqlite_data[3][0:21] == postgre_data[3][0:21]:
                find_element = True
        if not find_element:
            raise 'Списки не идентичны'

    sqlite_cursor.execute("SELECT id, film_work_id, person_id, role, created_at FROM person_film_work")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "SELECT id, film_work_id, person_id, role, TO_CHAR(created,'YYYY-MM-DD HH24:MI:SS.FF6+00') FROM content.person_film_work")
    postgre_person_data = postgre_cursor.fetchall()

    for sqlite_data in sqlite_person_data:
        find_element = False
        for postgre_data in postgre_person_data:
            if sqlite_data[0] == postgre_data[0] and sqlite_data[1] == postgre_data[1] \
                    and sqlite_data[2] == postgre_data[2] and sqlite_data[3] == postgre_data[3] \
                    and sqlite_data[4][0:21] == postgre_data[4][0:21]:
                find_element = True
        if not find_element:
            raise 'Списки не идентичны'


def check_structure(sqlite_cursor, postgre_cursor):
    """
    Проверка таблиц на соответствие по структуре.
    По названию полей и свойству is_nullable

    :param sqlite_cursor: Курсор к бд sqlite
    :type sqlite_cursor: cursor
    :param postgre_cursor: Курсор к бд postgre
    :type postgre_cursor: cursor
    """

    # PERSON
    sqlite_cursor.execute("PRAGMA table_info(person)")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "select column_name, data_type, is_nullable from information_schema.columns where table_schema = 'content' and table_name='person';")
    postgre_person_data = postgre_cursor.fetchall()

    column_names_postgre_sql = [properties_postgre_sql[0] for properties_postgre_sql in postgre_person_data]
    is_null_postgre_sql = [properties_postgre_sql[2] for properties_postgre_sql in postgre_person_data]

    column_names_sqlite_sql = [properties_sqlite_person[1] for properties_sqlite_person in sqlite_person_data]
    is_null_sqlite_sql = []
    for properties_sqlite_sql in sqlite_person_data:
        # в sqlite у pk всегда ставится nullable в свойствах
        if properties_sqlite_sql[3] == 1 or properties_sqlite_sql[5] == 1:
            is_null_sqlite_sql.append('NO')
        else:
            is_null_sqlite_sql.append('YES')

    # Сравниваем названия
    [print('В таблице Person в sqlite не найдена колонка:\n' + str(item)) for item in column_names_postgre_sql
     if item not in column_names_sqlite_sql]

    # Сравниваем свойство is_nullable
    for x in range(len(is_null_postgre_sql)):
        if is_null_postgre_sql[x] != is_null_sqlite_sql[x]:
            print('В таблице Person есть несоответствие свойства is nullable:\n' + str(is_null_postgre_sql[x]) + ' не равен ' + str(is_null_sqlite_sql[x]))

    # FILM_WORK
    sqlite_cursor.execute("PRAGMA table_info(film_work)")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "select column_name, data_type, is_nullable from information_schema.columns where table_schema = 'content' and table_name='film_work';")
    postgre_person_data = postgre_cursor.fetchall()

    column_names_postgre_sql = [properties_postgre_sql[0] for properties_postgre_sql in postgre_person_data]
    is_null_postgre_sql = [properties_postgre_sql[2] for properties_postgre_sql in postgre_person_data]

    column_names_sqlite_sql = [properties_sqlite_person[1] for properties_sqlite_person in sqlite_person_data]
    is_null_sqlite_sql = []
    for properties_sqlite_sql in sqlite_person_data:
        # в sqlite у pk всегда ставится nullable в свойствах
        if properties_sqlite_sql[3] == 1 or properties_sqlite_sql[5] == 1:
            is_null_sqlite_sql.append('NO')
        else:
            is_null_sqlite_sql.append('YES')

    # Сравниваем названия
    [print('В таблице Film_work в sqlite не найдена колонка:\n' + str(item)) for item in column_names_postgre_sql
     if item not in column_names_sqlite_sql]

    # Сравниваем свойство is_nullable
    nullable_count_postgre = len(
        [nullable_column for nullable_column in is_null_postgre_sql if nullable_column == 'NO']
    )
    nullable_count_sqlite = len(
        [nullable_column for nullable_column in is_null_sqlite_sql if nullable_column == 'NO']
    )
    if nullable_count_postgre != nullable_count_sqlite:
        print('Колво ненулевых колонок в таблице Film_work не совпадает')

    # GENRE
    sqlite_cursor.execute("PRAGMA table_info(genre)")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "select column_name, data_type, is_nullable from information_schema.columns where table_schema = 'content' and table_name='genre';")
    postgre_person_data = postgre_cursor.fetchall()

    column_names_postgre_sql = [properties_postgre_sql[0] for properties_postgre_sql in postgre_person_data]
    is_null_postgre_sql = [properties_postgre_sql[2] for properties_postgre_sql in postgre_person_data]

    column_names_sqlite_sql = [properties_sqlite_person[1] for properties_sqlite_person in sqlite_person_data]
    is_null_sqlite_sql = []
    for properties_sqlite_sql in sqlite_person_data:
        # в sqlite у pk всегда ставится nullable в свойствах
        if properties_sqlite_sql[3] == 1 or properties_sqlite_sql[5] == 1:
            is_null_sqlite_sql.append('NO')
        else:
            is_null_sqlite_sql.append('YES')

    # Сравниваем названия
    for x in range(len(column_names_postgre_sql)):
        if column_names_postgre_sql[x] != column_names_sqlite_sql[x]:
            print('В таблице GENRE есть несоответствие названий:\n' + str(column_names_postgre_sql[x]) + ' не равен ' + str(column_names_sqlite_sql[x]))

    # Сравниваем свойство is_nullable
    for x in range(len(is_null_postgre_sql)):
        if is_null_postgre_sql[x] != is_null_sqlite_sql[x]:
            print('В таблице GENRE есть несоответствие совйства is nullable:\n' + str(is_null_postgre_sql[x]) + ' не равен ' + str(is_null_sqlite_sql[x]))

    # GENRE_FILM_WORK
    sqlite_cursor.execute("PRAGMA table_info(genre_film_work)")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "select column_name, data_type, is_nullable from information_schema.columns where table_schema = 'content' and table_name='genre_film_work';")
    postgre_person_data = postgre_cursor.fetchall()

    column_names_postgre_sql = [properties_postgre_sql[0] for properties_postgre_sql in postgre_person_data]
    is_null_postgre_sql = [properties_postgre_sql[2] for properties_postgre_sql in postgre_person_data]

    column_names_sqlite_sql = [properties_sqlite_person[1] for properties_sqlite_person in sqlite_person_data]
    is_null_sqlite_sql = []
    for properties_sqlite_sql in sqlite_person_data:
        # в sqlite у pk всегда ставится nullable в свойствах
        if properties_sqlite_sql[3] == 1 or properties_sqlite_sql[5] == 1:
            is_null_sqlite_sql.append('NO')
        else:
            is_null_sqlite_sql.append('YES')

    # Сравниваем названия
    for x in range(len(column_names_postgre_sql)):
        if column_names_postgre_sql[x] != column_names_sqlite_sql[x]:
            print('В таблице GENRE_FILM_WORK есть несоответствие названий:\n' + str(column_names_postgre_sql[x]) + ' не равен ' + str(column_names_sqlite_sql[x]))

    # Сравниваем свойство is_nullable
    for x in range(len(is_null_postgre_sql)):
        if is_null_postgre_sql[x] != is_null_sqlite_sql[x]:
            print('В таблице GENRE_FILM_WORK есть несоответствие совйства is nullable:\n' + str(is_null_postgre_sql[x]) + ' не равен ' + str(is_null_sqlite_sql[x]))

    # PERSON_FILM_WORK
    sqlite_cursor.execute("PRAGMA table_info(person_film_work)")
    sqlite_person_data = sqlite_cursor.fetchall()

    postgre_cursor.execute(
        "select column_name, data_type, is_nullable from information_schema.columns where table_schema = 'content' and table_name='person_film_work';")
    postgre_person_data = postgre_cursor.fetchall()

    column_names_postgre_sql = [properties_postgre_sql[0] for properties_postgre_sql in postgre_person_data]
    is_null_postgre_sql = [properties_postgre_sql[2] for properties_postgre_sql in postgre_person_data]

    column_names_sqlite_sql = [properties_sqlite_person[1] for properties_sqlite_person in sqlite_person_data]
    is_null_sqlite_sql = []
    for properties_sqlite_sql in sqlite_person_data:
        # в sqlite у pk всегда ставится nullable в свойствах
        if properties_sqlite_sql[3] == 1 or properties_sqlite_sql[5] == 1:
            is_null_sqlite_sql.append('NO')
        else:
            is_null_sqlite_sql.append('YES')

    # Сравниваем названия
    for x in range(len(column_names_postgre_sql)):
        if column_names_postgre_sql[x] != column_names_sqlite_sql[x]:
            print('В таблице PERSON_FILM_WORK есть несоответствие названий:\n' + str(column_names_postgre_sql[x]) + ' не равен ' + str(column_names_sqlite_sql[x]))

    # Сравниваем свойство is_nullable
    for x in range(len(is_null_postgre_sql)):
        if is_null_postgre_sql[x] != is_null_sqlite_sql[x]:
            print('В таблице PERSON_FILM_WORK есть несоответствие совйства is nullable:\n' + str(is_null_postgre_sql[x]) + ' не равен ' + str(is_null_sqlite_sql[x]))


with psycopg2.connect(**dsn_postgre) as postgre_conn, sql.connect(dsn_sqlite['path_to_db'],) as sqlite_conn:
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_conn.row_factory = sql.Row

    postgre_conn.autocommit = False
    postgre_cursor = postgre_conn.cursor()

    # Сопоставляем колво данных в таблицах
    check_count(sqlite_cursor, postgre_cursor)
    # Сопоставляем значения в таблицах
    check_values(sqlite_cursor, postgre_cursor)
    # Сопоставляем названий и свойства is nullable в таблицах
    check_structure(sqlite_cursor, postgre_cursor)
