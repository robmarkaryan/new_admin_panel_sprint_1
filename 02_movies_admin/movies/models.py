import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')
        indexes = [
            models.Index(fields=['name'])
        ]

    def __str__(self):
        return self.name


class Filmwork(UUIDMixin, TimeStampedMixin):

    class FilmTypes(models.Model):
        film_types_list = [
            ('Movie', _('Film')),
            ('Tv show', _('Show')),
        ]

    title = models.CharField(_('title'), max_length=255)
    certificate = models.CharField(_('certificate'),
                                   null=True, max_length=512, blank=True)
    file_path = models.FileField(_('file'), blank=True,
                                 null=True, upload_to='movies/')
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation_date'))
    rating = models.FloatField(
        _('rating'),
        validators=[MinValueValidator(0), MaxValueValidator(10)])
    type = models.TextField(
        _('type'),
        choices=FilmTypes.film_types_list)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('film')
        verbose_name_plural = _('films')
        indexes = [
            models.Index(fields=['creation_date'])
        ]


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey(
        'Genre',
        on_delete=models.CASCADE,
        verbose_name=_('genre'))
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('fg')
        verbose_name_plural = _('fgs')
        indexes = [
            models.Index(fields=['genre_id']),
            models.Index(fields=['film_work_id'])
        ]

    def __str__(self):
        return 'жанр'


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.TextField(_('FIO'))
    birth_date = models.DateField(_('birth_date'), blank=True, null=True)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('people')
        indexes = [
            models.Index(fields=['full_name'])
        ]

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):
    class Roles(models.TextChoices):
        Director = 'D', _('Director')
        Writer = 'W', _('Writer')
        Actor = 'A', _('Actor')

    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey(
        'Person',
        on_delete=models.CASCADE,
        verbose_name=_('FIO'))
    role = models.TextField(_('role'), choices=Roles.choices)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('man in the film')
        verbose_name_plural = _('men in the film')

    def __str__(self):
        return self.role
