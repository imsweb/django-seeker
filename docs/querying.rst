Querying
========

Querying in Seeker is typically done on a per-mapping basis. Mapping instances can be retrieved using ``seeker.get_app_mappings(app_label)``
or ``seeker.get_model_mappings(model_class)``.


Querystring Searching
---------------------

Passing a string as the ``query`` parameter will perform a search using the `Elasticsearch query string syntax`_.

.. _`Elasticsearch query string syntax`: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax


Filtering
---------

Filtering is a process of reducing the number of results based on the values of fields, as opposed to full-text matching across fields.


Term Filtering
~~~~~~~~~~~~~~

Examples of term filtering::

    import seeker
    # The following two filter queries are equivalent:
    mapping.query(filters=seeker.F(author='Dan'))
    mapping.query(filters={'author': 'Dan'})

The ``F`` object can be used to combine term filters using boolean logic. For example::

    f = F(author='Dan') & (F(year=2014) | F(published=True))
    mapping.query(filters=f)

The above query will return results whose "author" field is "Dan", and either have published=True, or year=2014.


Range Filtering
~~~~~~~~~~~~~~~

The ``seeker.query.Range`` object is a subclass of ``F`` that allows filtering a field by min/max values::

    mapping.query(filters=seeker.Range('year', 2010, 2014))

By default, the range is inclusive on both ends, but the operators may be specified by passing ``min_oper`` and ``max_oper``
keyword arguments to the ``Range`` object. See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-filter.html
for more information.


Faceting/Aggregations
---------------------

Example::

    result = mapping.query(facets=[
        seeker.TermAggregate('author', size=40),
        seeker.YearHistogram('date_published'),
    ])
    for facet, values in result.facet_values():
        print facet, values


Highlighting
------------

TODO


Spelling Suggestions
--------------------

TODO


Sorting
-------

You may specify a field name to sort results by, optionally with an order separate by a colon (defaults to ``asc``). For example::

    mapping.query(query='some term', sort='name:desc')
