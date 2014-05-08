Views
=====

Basic View
----------

Seeker provides a Django class-based ``SeekerView`` that can be subclassed and customized for basic keyword searching and faceting.
To get started, you might define a view hooked up to :doc:`PostMapping <mapping>` directly in your ``urls.py``::

    from .mappings import PostMapping
    import seeker

    class PostSeekerView (seeker.SeekerView):
        mapping = PostMapping
    
    urlpatterns = patterns('',
        url(r'^posts/$', PostSeekerView.as_view(), name='posts'),
    )

By default, ``SeekerView`` renders a template named ``seeker/seeker.html``, which can be customized through subclassing. The included
template renders a fully-functional search page using Bootstrap and jQuery (hosted off CDNs).


Customizing Facets
------------------

By default, ``SeekerView`` displays facets for any ``MappingType`` with ``facet=True``. This includes foreign keys, many-to-many relationships,
and boolean fields if not otherwise specified. To customize which facets are executed and displayed, you may override the ``get_facets`` method::

    class PostSeekerView (seeker.SeekerView):
        mapping = PostMapping
        
        def get_facets(self):
            # Only facet on the "published" field, with a custom label.
            yield seeker.TermAggregate('published', label='Is Published?')
