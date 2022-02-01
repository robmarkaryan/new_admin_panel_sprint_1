import psycopg2
import sqlite3 as sql
import uuid
import datetime
from typing import List, Tuple, Any

from dataclasses import dataclass

dsn_postgre = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

dsn_sqlite = {
    'path_to_db': r'C:\Users\robma\PycharmProjects\admin_panel'
                  r'\03_sqlite_to_postgres\db.sqlite'
}


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    full_name: str
    birth_date: datetime.date
    created: datetime
    modified: datetime

    def return_tuple(self):
        return (self.id, self.full_name, self.birth_date,
                self.created, self.modified)


@dataclass(frozen=True)
class Film_work:
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime.date
    certificate: str
    file_path: str
    rating: float
    type: str
    created: datetime
    modified: datetime

    def return_tuple(self):
        return (self.id, self.title, self.description, self.creation_date,
                self.certificate, self.file_path, self.rating, self.type,
                self.created, self.modified
                )


@dataclass(frozen=True)
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created: datetime
    modified: datetime

    def return_tuple(self):
        return (self.id, self.name, self.description,
                self.created, self.modified)


@dataclass(frozen=True)
class Person_film_work:
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: datetime

    def return_tuple(self):
        return (self.id, self.film_work_id, self.person_id,
                self.role, self.created)


@dataclass(frozen=True)
class Genre_film_work:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime

    def return_tuple(self):
        return (self.id, self.film_work_id, self.genre_id, self.created)


def person_insert_query(person_data_sqlite: List[Person]):
    """
    Вставка данных в таблицу Person, бд Postgre

    :param person_data_sqlite: данные из таблицы Person, бд Sqlite
    """
    person_data_sqlite = split_list(person_data_sqlite, 1000)

    for person_data_sqlite_part in person_data_sqlite:

        args = ','.join(
            postgre_cursor.mogrify(
                "(%s, %s, %s, %s, %s)",
                item.return_tuple()
            ).decode() for item in person_data_sqlite_part
        )

        postgre_cursor.execute("""
        INSERT INTO content.person
        (id, full_name, birth_date, created, modified)
        VALUES {args}
        ON CONFLICT (id) DO NOTHING;;
        """.format(args=args))

        postgre_cursor.execute("""
        commit;
        """)


def film_work_insert_query(film_work_sqlite: List[Person]):
    """
    Вставка данных в таблицу Film_work, бд Postgre

    :param film_work_sqlite: данные из таблицы Film_work, бд Sqlite
    """
    film_work_sqlite = split_list(film_work_sqlite, 300)

    for film_work_sqlite_part in film_work_sqlite:

        args = ','.join(postgre_cursor.mogrify(
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            item.return_tuple()
        ).decode() for item in film_work_sqlite_part)

        postgre_cursor.execute("""
        INSERT INTO content.film_work (id
        ,title
        ,description
        ,creation_date
        ,certificate
        ,file_path
        ,rating
        ,type
        ,created
        ,modified)
        VALUES {args}
        ON CONFLICT (id) DO NOTHING;;
        """.format(args=args))

        postgre_cursor.execute("""
        commit;
        """)


def person_film_work_insert_query(person_film_work_sqlite: List[Person]):
    """
    Вставка данных в таблицу Person_film_work, бд Postgre

    :param person_film_work_sqlite:
    данные из таблицы Person_film_work, бд Sqlite
    """

    person_film_work_sqlite = split_list(person_film_work_sqlite, 1000)

    for person_film_work_sqlite_part in person_film_work_sqlite:
        args = ','.join(postgre_cursor.mogrify(
            "(%s, %s, %s, %s, %s)",
            item.return_tuple()
        ).decode() for item in person_film_work_sqlite_part)

        postgre_cursor.execute("""
        INSERT INTO content.person_film_work (id
        ,film_work_id
        ,person_id
        ,role
        ,created)
        VALUES {args}
        ON CONFLICT (id) DO NOTHING;;
        """.format(args=args))

        postgre_cursor.execute("""
        commit;
        """)


def genre_film_work_insert_query(genre_film_work_sqlite: List[Person]):
    """
    Вставка данных в таблицу Genre_film_work, бд Postgre

    :param genre_film_work_sqlite: данные из таблицы Genre_film_work, бд Sqlite
    """

    genre_film_work_sqlite = split_list(genre_film_work_sqlite, 1000)

    for genre_film_work_sqlite_part in genre_film_work_sqlite:
        args = ','.join(postgre_cursor.mogrify(
            "(%s, %s, %s, %s)",
            item.return_tuple()
        ).decode() for item in genre_film_work_sqlite_part)

        postgre_cursor.execute("""
        INSERT INTO content.genre_film_work (id
        ,film_work_id
        ,genre_id
        ,created)
        VALUES {args}
        ON CONFLICT (id) DO NOTHING;;
        """.format(args=args))

        postgre_cursor.execute("""
        commit;
        """)


def genre_insert_query(genre_sqlite: List[Person]):
    """
    Вставка данных в таблицу Genre, бд Postgre

    :param genre_sqlite: данные из таблицы Genre, бд Sqlite
    """
    genre_sqlite = split_list(genre_sqlite, 5)

    for genre_sqlite_part in genre_sqlite:
        args = ','.join(postgre_cursor.mogrify(
            "(%s, %s, %s, %s, %s)",
            item.return_tuple()
        ).decode() for item in genre_sqlite_part)

        postgre_cursor.execute("""
        INSERT INTO content.genre (id
        ,name
        ,description
        ,created
        ,modified)
        VALUES {args}
        ON CONFLICT (id) DO NOTHING;;
        """.format(args=args))

        postgre_cursor.execute("""
        commit;
        """)


def add_person_to_class(person_data: List[Tuple]) -> List[Person]:
    """
    Инициализация класса Person

    :param person_data: список из таблицы Person, бд Sqlite
    :return person_data_sqlite: список с инициализированным классом Person
    """
    person_data_sqlite = []
    for person in person_data:
        person_data_sqlite.append(Person(id=person[0],
                                         full_name=person[1],
                                         birth_date=person[2],
                                         created=person[3],
                                         modified=person[4]))

    return person_data_sqlite


def add_film_work_to_class(film_work_data: List[Tuple]) -> List[Film_work]:
    """
    Инициализация класса Film_work

    :param film_work_data: список из таблицы Film_work, бд Sqlite
    :return film_work_sqlite: список с инициализированным классом Film_work
    """

    film_work_sqlite = []
    for film_work in film_work_data:
        film_work_sqlite.append(Film_work(id=film_work[0],
                                          title=film_work[1],
                                          description=film_work[2],
                                          creation_date=film_work[3],
                                          certificate=film_work[4],
                                          file_path=film_work[5],
                                          rating=film_work[6],
                                          type=film_work[7],
                                          created=film_work[8],
                                          modified=film_work[9]))

    return film_work_sqlite


def add_genre_to_class(genre_data: List[Tuple]) -> List[Genre]:
    """
    Инициализация класса Genre

    :param genre_data: список из таблицы Genre, бд Sqlite
    :return genre_data: список с инициализированным классом Genre
    """

    genre_sqlite = []
    for genre in genre_data:
        genre_sqlite.append(
            Genre(
                    id=genre[0],
                    name=genre[1],
                    description=genre[2],
                    created=genre[3],
                    modified=genre[4]
                  )
        )

    return genre_sqlite


def add_person_film_work_to_class(person_film_work_data: List[Tuple]) -> \
        List[Person_film_work]:
    """
    Инициализация класса Person_film_work

    :param person_film_work_data:
    список из таблицы Person_film_work, бд Sqlite
    :return person_film_work_sqlite:
    список с инициализированным классом Person_film_work
    """

    person_film_work_sqlite = []
    for person_film_work in person_film_work_data:
        person_film_work_sqlite.append(
            Person_film_work(
                id=person_film_work[0],
                film_work_id=person_film_work[1],
                person_id=person_film_work[2],
                role=person_film_work[3],
                created=person_film_work[4]
            )
        )

    return person_film_work_sqlite


def add_genre_film_work_to_class(genre_film_work_data):
    """
    Инициализация класса Person_film_work

    :param genre_film_work_data: список из таблицы Genre_film_work, бд Sqlite
    :return genre_film_work_sqlite:
    список с инициализированным классом Genre_film_work
    """

    genre_film_work_sqlite = []
    for genre_film_work in genre_film_work_data:
        genre_film_work_sqlite.append(
            Genre_film_work(
                id=genre_film_work[0],
                film_work_id=genre_film_work[1],
                genre_id=genre_film_work[2],
                created=genre_film_work[3]
            )
        )

    return genre_film_work_sqlite


def split_list(list_to_be_divided: List[Any], count: int) -> List[Any]:
    """

    :param list_to_be_divided: Список, который требуется разделить
    :param count: приблизительное кол-во элементов на запись
    :return divided_list: список разделенный на части
    """

    parts = len(list_to_be_divided)//count
    step = len(list_to_be_divided) / float(parts)
    divided_list = []
    last = 0

    while last < len(list_to_be_divided):
        divided_list.append(list_to_be_divided[int(last):int(last + step)])
        last += step

    return divided_list


with psycopg2.connect(**dsn_postgre) as postgre_conn, \
        sql.connect(dsn_sqlite['path_to_db']) as sqlite_conn:
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_conn.row_factory = sql.Row

    # Чтение данных из бд sqlite

    try:
        # Получаем все строки из таблицы person
        sqlite_cursor.execute("SELECT * FROM person")
        person_data = sqlite_cursor.fetchall()

        person_data_sqlite = add_person_to_class(person_data)

        # Получаем все строки из таблицы film_work
        sqlite_cursor.execute("SELECT * FROM film_work")
        film_work_data = sqlite_cursor.fetchall()

        film_work_sqlite = add_film_work_to_class(film_work_data)

        # Получаем все строки из таблицы person_film_work
        sqlite_cursor.execute("SELECT * FROM person_film_work")
        person_film_work_data = sqlite_cursor.fetchall()

        person_film_work_sqlite = add_person_film_work_to_class(
            person_film_work_data
        )

        # Получаем все строки из таблицы genre
        sqlite_cursor.execute("SELECT * FROM genre")
        genre_data = sqlite_cursor.fetchall()

        genre_sqlite = add_genre_to_class(genre_data)

        # Получаем все строки из таблицы genre_film_work
        sqlite_cursor.execute("SELECT * FROM genre_film_work")
        genre_film_work_data = sqlite_cursor.fetchall()

        genre_film_work_sqlite = add_genre_film_work_to_class(
            genre_film_work_data
        )

    except Exception as exception:
        error = exception

    # Начинаем вставку данных
    try:

        postgre_conn.autocommit = False

        postgre_cursor = postgre_conn.cursor()

        person_insert_query(person_data_sqlite)

        film_work_insert_query(film_work_sqlite)

        genre_insert_query(genre_sqlite)

        person_film_work_insert_query(person_film_work_sqlite)

        genre_film_work_insert_query(genre_film_work_sqlite)
    except Exception as exception:
        error = exception
