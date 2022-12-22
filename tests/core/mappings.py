from seeker.dsl import dsl

import seeker

from tests.core.external import BaseDocument
from tests.core.models import Book, Magazine

BookDocument = seeker.document_from_model(Book, module=__name__)
MagazineDocument = seeker.document_from_model(Magazine, module=__name__)


class DjangoBookDocument(seeker.ModelIndex):

    class Meta:
        mapping = seeker.build_mapping(Book)

    class Index:
        name = 'djangobook'

    @classmethod
    def queryset(cls):
        return Book.objects.filter(title__icontains='django')


class DerivedDocument(BaseDocument):
    derived_field = dsl.Integer()

    class Index:
        name = 'derived'
