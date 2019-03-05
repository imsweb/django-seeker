import logging
import threading

from .mapping import Indexable, ModelIndex


logger = logging.getLogger(__name__)


documents = []

model_documents = {}
model_doc_types = {}
app_documents = {}


def register(doc_class, app_label=None):
    assert issubclass(doc_class, Indexable)
    if doc_class in documents:
        logger.warning('Document class %s.%s was previously registered - skipping.', doc_class.__module__, doc_class.__name__)
        return
    documents.append(doc_class)
    if issubclass(doc_class, ModelIndex):
        model_class = doc_class.model
        if not model_class:    
            model_class = doc_class.queryset().model
        # It's possible to register more than one document type for a model, so keep a list.
        model_documents.setdefault(model_class, []).append(doc_class)
        # For doing queries across multiple document types, we'll need a mapping from doc_type back to model_class.
        model_doc_types[doc_class._doc_type.name] = model_class
    if app_label:
        app_documents.setdefault(app_label, []).append(doc_class)
