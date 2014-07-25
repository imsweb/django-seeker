Configuration
=============

Seeker Settings
---------------

.. _setting-seeker-hosts:

``SEEKER_HOSTS`` (default: ``None``)
    A list of ES hosts to connect to by default. This can be overridden per mapping.

.. _setting-seeker-index:

``SEEKER_INDEX`` (default: seeker)
    The name of the ES index that should be used by default. This can be overridden per mapping.

.. _setting-seeker-default-operator:

``SEEKER_DEFAULT_OPERATOR`` (default: ``OR``)
    The default operator to use when performing keyword queries.

``SEEKER_HTTP_AUTH`` (default: ``None``)
    A colon-separated username and password to use for HTTP basic authentication.

``SEEKER_SAVED_SEARCHES`` (default: ``True``)
    Whether the ``SavedSearch`` model should be installed, and present the option to save searches in the default form.

``SEEKER_BATCH_SIZE`` (default: 1000)
    The default indexing batch size.


Model Indexing Middleware
-------------------------

For sites that want model instances to be automatically indexed when they are created, updated, or deleted, Seeker includes
a ``ModelIndexingMiddleware`` that connects to Django's ``post_save`` and ``post_delete`` signals. To use it, simply add
``seeker.middleware.ModelIndexingMiddleware`` to your ``MIDDLEWARE_CLASSES`` setting above any middleware that might alter
model instances you want indexed.

Models are not automatically indexed when outside of a request cycle (with ``ModelIndexingMiddleware`` installed), to prevent
unwanted or premature indexing during load scripts, bulk updates, etc. Instances may be indexed manually using ``seeker.index``.
If automatic updating is desired outside of the request cycle, it is possible to simply instantiate ``ModelIndexingMiddleware``
and keep a reference to it. The class connects to ``post_save`` and ``post_delete`` when created, so you may do something like::

    from seeker.middleware import ModelIndexingMiddleware
    middleware = ModelIndexingMiddleware()
    # Update your model instances as necessary, they will be automatically indexed.
    del middleware
