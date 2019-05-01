from __future__ import division

import importlib
import sys
import time

import elasticsearch_dsl as dsl
from django.conf import settings
from django.http import QueryDict
from django.utils.encoding import force_text
from elasticsearch import NotFoundError
from elasticsearch_dsl.connections import connections

from .registry import model_documents


def import_class(fq_name):
    module_name, class_name = fq_name.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)


def index(obj, index=None, using=None):
    """
    Shortcut to index a Django object based on it's model class.
    """
    from django.contrib.contenttypes.models import ContentType
    model_class = ContentType.objects.get_for_model(obj).model_class()
    for doc_class in model_documents.get(model_class, []):
        if not doc_class.queryset().filter(pk=obj.pk).exists():
            continue
        doc_using = using or doc_class._index._using or 'default'
        doc_index = index or doc_class._index._name
        es = connections.get_connection(doc_using)
        body = doc_class.serialize(obj)
        doc_id = body.pop('_id', None)
        es.index(
            index=doc_index,
            doc_type=doc_class._doc_type.name,
            body=body,
            id=doc_id,
            refresh=True
        )


def delete(obj, index=None, using=None):
    """
    Shortcut to delete a Django object from the ES index based on it's model class.
    """
    from django.contrib.contenttypes.models import ContentType
    model_class = ContentType.objects.get_for_model(obj).model_class()
    for doc_class in model_documents.get(model_class, []):
        doc_using = using or doc_class._index._using or 'default'
        doc_index = index or doc_class._index._name
        es = connections.get_connection(doc_using)
        try:
            es.delete(
                index=doc_index,
                doc_type=doc_class._doc_type.name,
                id=doc_class.get_id(obj),
                refresh=True
            )
        except NotFoundError:
            # If this object wasn't indexed for some reason (maybe not in the document's queryset), no big deal.
            pass


def search(models=None, using='default'):
    """
    Returns a search object across the specified models.
    """
    types = []
    indices = []
    if models is None:
        models = model_documents
    for model_class in models:
        for doc_class in model_documents.get(model_class, []):
            indices.append(doc_class._index._name)
            types.append(doc_class)
    return dsl.Search(using=using).index(*indices).doc_type(*types)


def progress(iterator, count=None, label='', size=40, chars='# ', output=sys.stdout, frequency=1.0):
    """
    An iterator wrapper that writes/updates a progress bar to an output stream (stdout by default).
    Based on http://code.activestate.com/recipes/576986-progress-bar-for-console-programs-as-iterator/
    """
    assert len(chars) >= 2
    if label:
        label = force_text(label) + ' '

    try:
        count = len(iterator)
    except Exception:
        pass

    start = time.time()

    def show(i):
        if count:
            x = int(size * i // count)
            bar = '[%s%s]' % (chars[0] * x, chars[1] * (size - x))
            pct = int((100.0 * i) // count)
            status = '%s/%s %s%%' % (i, count, pct)
        else:
            bar = ''
            status = str(i)
        e = time.time() - start
        mins, s = divmod(int(e), 60)
        h, m = divmod(mins, 60)
        elapsed = '%d:%02d:%02d' % (h, m, s) if h else '%02d:%02d' % (m, s)
        speed = '%.2f iters/sec' % (i / e) if e > 0 else ''
        output.write('%s%s %s - %s, %s\r' % (label, bar, status, elapsed, speed))
        output.flush()

    show(0)
    last_update = 0.0
    processed = 0
    for item in iterator:
        yield item
        processed += 1
        since = time.time() - last_update
        if since >= frequency:
            show(processed)
            last_update = time.time()
    show(processed)

    output.write('\n')
    output.flush()


def convert_saved_search_to_search_object(saved_search):
    """
    This function helps create a search_object from a SavedSearch.
    This util function can be helpful when upgrading from SavedSearch to AdvancedSavedSearch
    """
    querystring_list_keys = {'d': 'display', 'f': 'selected_facets', 'so': 'displaySortOrder'}
    querystring_string_keys = {'s': 'sort', 'q': 'keywords', 'p': 'page'}
    data = QueryDict(saved_search.querystring)
    search_object = {querystring_list_keys[key]: data.getlist(key) for key in querystring_list_keys}
    search_object.update({querystring_string_keys[key]: data.get(key, '') for key in querystring_string_keys})
    search_object['url'] = saved_search.url
    search_object['page'] = int(search_object['page']) if search_object['page'] else 1
    rules = []
    for facet in search_object['selected_facets']:
        values = data.getlist(facet)
        facet_rules = []
        for value in values:
            facet_rules.append({'operator': 'equal', 'id': facet, 'value': value})
        rules.append({'rules': facet_rules, 'condition': "OR"})
    search_object['query'] = {"condition": "AND", "rules": rules}

    return search_object
