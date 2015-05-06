from .mapping import Indexable, ModelIndex

documents = []

model_documents = {}
model_doc_types = {}

def register(doc_class):
    assert issubclass(doc_class, Indexable)
    documents.append(doc_class)
    if issubclass(doc_class, ModelIndex):
        # For ModelIndex documents, store some extra information.
        model_class = doc_class.queryset().model
        # It's possible to register more than one document type for a model, so keep a list.
        model_documents.setdefault(model_class, []).append(doc_class)
        # For doing queries across multiple document types, we'll need a mapping from doc_type back to model_class.
        model_doc_types[doc_class._doc_type.name] = model_class
