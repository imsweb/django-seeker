from .registry import model_documents
import elasticsearch_dsl as dsl
import time
import sys

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

def progress(iterator, count=None, label='', size=40, chars='# ', output=sys.stdout, frequency=0.0):
    """
    An iterator wrapper that writes/updates a progress bar to an output stream (stdout by default).
    Based on http://code.activestate.com/recipes/576986-progress-bar-for-console-programs-as-iterator/
    """
    assert len(chars) >= 2
    if label:
        label = unicode(label) + ' '

    try:
        count = len(iterator)
    except:
        pass

    start = time.time()

    def show(i):
        if count:
            x = int(size * i / count)
            bar = '[%s%s]' % (chars[0] * x, chars[1] * (size - x))
            pct = int((100.0 * i) / count)
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
    for num, item in enumerate(iterator):
        yield item
        since = time.time() - last_update
        if since >= frequency:
            show(num + 1)
            last_update = time.time()

    output.write('\n')
    output.flush()
