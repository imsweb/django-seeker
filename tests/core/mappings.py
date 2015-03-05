import seeker
import elasticsearch_dsl
from .models import Book, Magazine

class BookDocument (elasticsearch_dsl.DocType, seeker.Indexable):
    class Meta:
        index = 'seeker-tests'
        doc_type = 'book'

MagazineDocument = seeker.document_from_model(Magazine)

seeker.register(Book, BookDocument)
seeker.register(Magazine, MagazineDocument)
