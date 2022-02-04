import uuid
from dataclasses import dataclass
import datetime


@dataclass
class Person:
    id: uuid.UUID = None
    full_name: str = None
    birth_date: datetime.date = None
    created: datetime = None
    modified: datetime = None

    def return_tuple(self):
        return (self.id, self.full_name, self.birth_date,
                self.created, self.modified)


@dataclass
class Film_work:
    id: uuid.UUID = None
    title: str = None
    description: str = None
    creation_date: datetime.date = None
    certificate: str = None
    file_path: str = None
    rating: float = None
    type: str = None
    created: datetime = None
    modified: datetime = None

    def return_tuple(self):
        return (self.id, self.title, self.description, self.creation_date,
                self.certificate, self.file_path, self.rating, self.type,
                self.created, self.modified
                )


@dataclass
class Genre:
    id: uuid.UUID = None
    name: str = None
    description: str = None
    created: datetime = None
    modified: datetime = None

    def return_tuple(self):
        return (self.id, self.name, self.description,
                self.created, self.modified)


@dataclass
class Genre_film_work:
    id: uuid.UUID = None
    film_work_id: uuid.UUID = None
    genre_id: uuid.UUID = None
    created: datetime = None

    def return_tuple(self):
        return (self.id, self.film_work_id, self.genre_id, self.created)


@dataclass
class Person_film_work:
    id: uuid.UUID = None
    film_work_id: uuid.UUID = None
    person_id: uuid.UUID = None
    role: str = None
    created: datetime = None

    def return_tuple(self):
        return (self.id, self.film_work_id, self.person_id,
                self.role, self.created)
