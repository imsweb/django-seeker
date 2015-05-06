import seeker
import elasticsearch_dsl as dsl
from .models import Book, Magazine

BookDocument = seeker.document_from_model(Book)
MagazineDocument = seeker.document_from_model(Magazine)

seeker.register(BookDocument)
seeker.register(MagazineDocument)
