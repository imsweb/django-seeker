from .registry import model_documents
import elasticsearch_dsl as dsl

def get_mappings(model=None):
    """
    Returns a list of all registered mappings, optionally filtered by a model class.
    """
    if model:
        return model_documents.get(model, [])
    else:
        mappings = []
        for mapping_list in model_documents.values():
            mappings.extend(mapping_list)
        return mappings

def index(obj):
    """ Shortcut to index an object based on it's model class. """
    # TODO: should this use ContentType, to deal with proxy models?
    for mapping in get_mappings(obj.__class__):
        mapping.index(obj) # FIXME

def search(models=None, using='default'):
    """
    Returns a search object across the specified models.
    """
    types = []
    indices = []
    if models is None:
        models = list(model_documents.keys())
    for model_class in models:
        for d in model_documents.get(model_class, []):
            indices.append(d._doc_type.index)
            types.append(d._doc_type.name)
    # TODO: callbacks?
    return dsl.Search(using=using).index(*indices).doc_type(*types)

def queryset(model_class, **kwargs):
    """
    Given a model class and keyword query arguments, returns a Django QuerySet by first querying Elasticsearch for a list of
    IDs, then calling ``mapping.queryset().filter(pk__in=<id_list>)``.
    """
    return get_mappings(model_class)[0].query(**kwargs).queryset() # FIXME
