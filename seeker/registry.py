from django.apps import apps
from django.core.exceptions import ImproperlyConfigured

from .mapping import Indexable, ModelIndex

import threading

documents = []

model_documents = {}
model_doc_types = {}
app_documents = {}

current_app = threading.local()

REGISTER_ERROR = "Seeker mapping registration failed.\nCould not find App label for mapping: {0}\nExpected installed app with name == '{1}'"

def register(doc_class):
    assert issubclass(doc_class, Indexable)
    app_lkup = {app.name: app.label for app in apps.get_app_configs()}
    try:
        doc_module = '.'.join(doc_class.__module__.split('.')[:-1])
        label = app_lkup[doc_module]
    except KeyError:
        raise ImproperlyConfigured(REGISTER_ERROR.format(doc_class, doc_module))
    if doc_class in documents:
        return
    documents.append(doc_class)
    if issubclass(doc_class, ModelIndex):
        model_class = doc_class.queryset().model
        # It's possible to register more than one document type for a model, so keep a list.
        model_documents.setdefault(model_class, []).append(doc_class)
        # For doing queries across multiple document types, we'll need a mapping from doc_type back to model_class.
        model_doc_types[doc_class._doc_type.name] = model_class
    app_documents.setdefault(label, []).append(doc_class)
