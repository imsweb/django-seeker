import elasticsearch_dsl as dsl

import seeker

from .external import BaseDocument
from .models import Book, Magazine

BookDocument = seeker.document_from_model(Book, module=__name__)
MagazineDocument = seeker.document_from_model(Magazine, module=__name__)

class DerivedDocument (BaseDocument):
    derived_field = dsl.Integer()
