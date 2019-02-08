Views
=====

Basic View
----------

Seeker provides a Django class-based ``SeekerView`` that can be subclassed and customized for basic keyword searching
and faceting. To get started, you might define a view hooked up to :doc:`PostMapping <mapping>`::

    from .mappings import PostDoc
    import seeker

    class PostSeekerView(seeker.SeekerView):
        document = PostDoc

    urlpatterns = patterns('',
        url(r'^posts/$', PostSeekerView.as_view(), name='posts'),
    )

By default, ``SeekerView`` renders a template named ``seeker/seeker.html``, which can be customized through subclassing.
The included template renders a fully-functional search page using Bootstrap and jQuery (hosted off CDNs).


Customizing Facets
------------------

TODO


Class Reference
---------------

.. autoclass:: seeker.views.SeekerView
    :members:
