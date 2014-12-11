from .mapping import Indexable

model_documents = {}

def register(model_class, doc_class):
    assert issubclass(doc_class, Indexable)
    setattr(doc_class, '_model', model_class)
    model_documents[model_class] = doc_class
