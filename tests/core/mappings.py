from .models import Book, Magazine
import seeker

BookDocument = seeker.document_from_model(Book)
MagazineDocument = seeker.document_from_model(Magazine)

seeker.register(BookDocument)
seeker.register(MagazineDocument)
