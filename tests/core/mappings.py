import seeker

from .models import Book, Magazine


BookDocument = seeker.document_from_model(Book)
MagazineDocument = seeker.document_from_model(Magazine)


class DjangoBookDocument (seeker.ModelIndex):

    class Meta:
        mapping = seeker.build_mapping(Book, doc_type='django_book')

    @classmethod
    def queryset(cls):
        return Book.objects.filter(title__icontains='django')


seeker.register(BookDocument)
seeker.register(MagazineDocument)
seeker.register(DjangoBookDocument)
