import data_model as dm
import settings
import psycopg2
import sqlite3 as sql
from dotenv import load_dotenv


load_dotenv()

def insert_query(batches_sqlite):
    """
    Вставка данных в таблицу, бд Postgre

    :param batches_sqlite: данные из таблицы, бд Sqlite
    """

    column_names_list = list(batches_sqlite[0][0].__dict__.keys())
    column_names_list_str = ', '.join(column_names_list)
    schema_table_name = 'content.' + batches_sqlite[0][0].__class__.__name__

    values = ['%s' for i in range(0, len(column_names_list))]
    values_str = ', '.join(values)

    for batch in batches_sqlite:
        args = ','.join(
            postgre_cursor.mogrify(
                "({args})".format(args=values_str),
                item.return_tuple()
            ).decode() for item in batch
        )

        postgre_cursor.execute("""
        INSERT INTO {schema_table_name}
        ({column_names})
        VALUES {values}
        ON CONFLICT (id) DO NOTHING;;
        """.format(schema_table_name=schema_table_name, column_names=column_names_list_str, values=args))

        postgre_cursor.execute("""
        commit;
        """)


def add_to_class(sqlite_data, class_instances_list):

    list_of_data = []
    column_names = sqlite_data[0].keys()
    for data_index in range(len(sqlite_data)):
        for column_name in column_names:
            setattr(class_instances_list[data_index], column_name, sqlite_data[data_index][column_name])
        list_of_data.append(class_instances_list[data_index])

    return list_of_data

if __name__ == '__main__':
    with psycopg2.connect(**settings.dsn_postgre) as postgre_conn, \
            sql.connect(settings.dsn_sqlite['path_to_db']) as sqlite_conn:
        sqlite_conn.row_factory = sql.Row
        sqlite_cursor = sqlite_conn.cursor()

        # Чтение данных из бд sqlite
        try:
            size_of_fetch = 500
            # Получаем все строки из таблицы person
            sqlite_cursor.execute("SELECT id, full_name, birth_date, created_at as created, updated_at as modified FROM person")
            person_batches_sqlite = []
            while True:
                person_data = sqlite_cursor.fetchmany(size_of_fetch)
                if not person_data:
                    break
                else:
                    person_list = [dm.Person() for row in range(size_of_fetch)]
                    person_batches_sqlite.append(add_to_class(person_data, person_list))

            # Получаем все строки из таблицы film_work
            sqlite_cursor.execute("SELECT id, title, description, creation_date, certificate, file_path, rating, type, created_at as created, updated_at as modified FROM film_work;")
            film_work_batches_sqlite = []
            while True:
                film_work_data = sqlite_cursor.fetchmany(size_of_fetch)
                if not film_work_data:
                    break
                else:
                    film_work_list = [dm.Film_work() for row in range(size_of_fetch)]
                    film_work_batches_sqlite.append(add_to_class(film_work_data, film_work_list))

            # Получаем все строки из таблицы person_film_work
            sqlite_cursor.execute("SELECT id, film_work_id, person_id, role, created_at as created FROM person_film_work")
            person_film_work_batches_sqlite = []
            while True:
                person_film_work_data = sqlite_cursor.fetchmany(size_of_fetch)
                if not person_film_work_data:
                    break
                else:
                    person_film_work_list = [dm.Person_film_work() for row in range(size_of_fetch)]
                    person_film_work_batches_sqlite.append(add_to_class(person_film_work_data, person_film_work_list))

            # Получаем все строки из таблицы genre
            sqlite_cursor.execute("SELECT id, name, description, created_at as created, updated_at as modified FROM genre")
            genre_batches_sqlite = []
            while True:
                genre_data = sqlite_cursor.fetchmany(size_of_fetch)
                if not genre_data:
                    break
                genre_list = [dm.Genre() for row in range(size_of_fetch)]
                genre_batches_sqlite.append(add_to_class(genre_data, genre_list))

            # Получаем все строки из таблицы genre_film_work
            sqlite_cursor.execute("SELECT id, film_work_id, genre_id, created_at as created FROM genre_film_work")
            genre_film_work_batches_sqlite = []
            while True:
                genre_film_work_data = sqlite_cursor.fetchmany(size_of_fetch)
                if not genre_film_work_data:
                    break
                genre_film_work_list = [dm.Genre_film_work() for row in range(size_of_fetch)]
                genre_film_work_batches_sqlite.append(add_to_class(genre_film_work_data, genre_film_work_list))

        except Exception as exception:
            error = exception
            print(exception)

        # Начинаем вставку данных
        try:

            postgre_conn.autocommit = False
            postgre_cursor = postgre_conn.cursor()

            insert_query(person_batches_sqlite)
            insert_query(film_work_batches_sqlite)
            insert_query(genre_batches_sqlite)
            insert_query(person_film_work_batches_sqlite)
            insert_query(genre_film_work_batches_sqlite)
        except Exception as exception:
            error = exception
            print(exception)

    postgre_conn.close()
    sqlite_conn.close()
