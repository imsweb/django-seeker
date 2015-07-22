from .mapping import Indexable, ModelIndex
import threading

documents = []

model_documents = {}
model_doc_types = {}
app_documents = {}

current_app = threading.local()

def register(doc_class):
    assert issubclass(doc_class, Indexable)
    if doc_class in documents:
        return
    documents.append(doc_class)
    if issubclass(doc_class, ModelIndex):
        # It's possible to register more than one document type for a model, so keep a list.
        model_documents.setdefault(doc_class.model, []).append(doc_class)
        # For doing queries across multiple document types, we'll need a mapping from doc_type back to model_class.
        model_doc_types[doc_class._doc_type.name] = doc_class.model
    try:
        app_documents.setdefault(current_app.label, []).append(doc_class)
    except:
        pass
