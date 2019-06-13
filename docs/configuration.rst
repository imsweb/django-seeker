Configuration
=============

Seeker Settings
---------------

SEEKER_INDEX
~~~~~~~~~~~~

Default: ``seeker``

The name of the ES index that should be used by default. This can be overridden per mapping.


SEEKER_INDEX_PREFIX
~~~~~~~~~~~~

Default: ``seeker``

The PREFIX used for each ES index created.


SEEKER_INDEX_SETTINGS
~~~~~~~~~~~~

Default: ``{}``

Default settings to be used for Indexes

SEEKER_DEFAULT_OPERATOR
~~~~~~~~~~~~~~~~~~~~~~~

Default: ``AND``

The default operator to use when performing keyword queries. This can be overridden per view.


SEEKER_BATCH_SIZE
~~~~~~~~~~~~~~~~~

Default: ``1000``

The default indexing batch size.


SEEKER_DEFAULT_FACET_TEMPLATE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``seeker/facets/terms.html``

The default template to use when rendering facets. Can be overridden per facet.


SEEKER_MAPPING_MODULE
~~~~~~~~~~~~~~~~~~~~~

Default: ``mappings``

The name of the python module to try to automatically import from each app. Setting to ``False`` or ``None`` will cause
seeker to skip doing any automatic imports.


SEEKER_DEFAULT_ANALYZER
~~~~~~~~~~~~~~~~~~~~~~~

Default: ``snowball``

The analyzer to use by default when creating ``elasticsearch_dsl.String`` fields. Also used by default in ``SeekerView``
to determine how query strings should be analyzed (it's important that queries are analyzed the same way as your data).


Model Indexing Middleware
-------------------------

For sites that want model instances to be automatically indexed when they are created, updated, or deleted, Seeker
includes a ``ModelIndexingMiddleware`` that connects to Django's ``post_save`` and ``post_delete`` signals. To use it,
simply add ``seeker.middleware.ModelIndexingMiddleware`` to your ``MIDDLEWARE_CLASSES`` setting above any middleware
that might alter model instances you want indexed.

Models are not automatically indexed when outside of a request cycle (with ``ModelIndexingMiddleware`` installed), to
prevent unwanted or premature indexing during load scripts, bulk updates, etc. Instances may be indexed manually using
``seeker.index``. If automatic updating is desired outside of the request cycle, it is possible to simply instantiate
``ModelIndexingMiddleware`` and keep a reference to it. The class connects to ``post_save`` and ``post_delete`` when
created, so you may do something like::

    from seeker.middleware import ModelIndexingMiddleware
    middleware = ModelIndexingMiddleware()
    # Update your model instances as necessary, they will be automatically indexed.
    del middleware
