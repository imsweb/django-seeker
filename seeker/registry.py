from .mapping import Indexable

model_documents = {}
doc_types = {}

def register(model_class, doc_class):
    assert issubclass(doc_class, Indexable)
    # This is set automatically if you use document_from_model, but not if composing a DocType with an Indexable mix-in.
    setattr(doc_class, '_model', model_class)
    # It's possible to register more than one document type for a model, so keep a list.
    model_documents.setdefault(model_class, []).append(doc_class)
    # For doing queries across multiple document types, we'll need a mapping from doc_type back to model_class.
    doc_types[doc_class._doc_type.name] = model_class
