from .registry import model_documents
import elasticsearch_dsl as dsl

def index(obj):
    """
    Shortcut to index a Django object based on it's model class.
    """
    for doc_class in model_documents.get(obj.__class__, []):
        data = doc_class.serialize(obj)
        doc_class(**data).save()

def search(models=None, using='default'):
    """
    Returns a search object across the specified models.
    """
    types = []
    indices = []
    if models is None:
        models = model_documents.keys()
    for model_class in models:
        for doc_class in model_documents.get(model_class, []):
            indices.append(doc_class._doc_type.index)
            types.append(doc_class)
    return dsl.Search(using=using).index(*indices).doc_type(*types)
