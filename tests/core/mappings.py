import seeker
from .models import Book

class BookMapping (seeker.Mapping):
    model = Book
