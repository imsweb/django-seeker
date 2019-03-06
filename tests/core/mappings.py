import seeker
from .models import Book, Magazine


class BookMapping (seeker.Mapping):
    model = Book


class MagazineMapping (seeker.Mapping):
    model = Magazine
