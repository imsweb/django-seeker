Mappings
========

Our Example Model
-----------------

For the purposes of this document, take the following models::

    class Author (models.Model):
        name = models.CharField(max_length=100)
        age = models.IntegerField()

        def __unicode__(self):
            return self.name

    class Post (models.Model):
        author = models.ForeignKey(Author, related_name='posts')
        slug = models.SlugField()
        title = models.CharField(max_length=100)
        body = models.TextField()
        date_posted = models.DateTimeField(default=timezone.now)
        published = models.BooleanField(default=True)


.. _basic-mappings:

Basic Documents
---------------

Documents are analogous to Django models, but for Elasticsearch instead of a database. For the simplest cases, you can
let seeker define the document for you, indexing any field it can::

    import seeker
    from .models import Post

    PostDoc = seeker.document_from_model(Post)
    seeker.register(PostDoc)

Most built-in Django field types are automatically indexed, including ``ForeignKey`` and ``ManyToManyField`` (using
their unicode representations).


The Registry
------------

In order for seeker to know about a document for indexing purposes, you need to register it.


Customizing Field Mappings
--------------------------

You can specify how seeker builds the mapping for your model class in several ways::

    import elasticsearch_dsl as dsl

    class PostDoc (seeker.ModelIndex):
        # Custom field definition for existing field
        author = dsl.Object(properties={
            'name': seeker.RawString,
            'age': dsl.Integer(),
        })
        # New field not defined by the model
        word_count = dsl.Long()

        class Meta:
            mapping = seeker.build_mapping(Post, fields=('title', 'body'), exclude=('slug',))

        @classmethod
        def queryset(cls):
            return Post.objects.select_related('author')

Think of ``Meta.mapping`` as the "base" set of fields, which you can then customize by defining them directly on the document class.
Any field defined on your document class will take precedence over those built in ``Meta.mapping`` with the same name, and any new fields
will be added to the mapping.

Notice in the example above that ``author`` is overridden to use `Elasticsearch object type`_, and ``word_count`` is an extra field not
defined by the ``Post`` model.

.. _`Elasticsearch object type`: https://www.elastic.co/guide/en/elasticsearch/reference/1.7/mapping-object-type.html


Indexing Data
-------------

When Seeker goes to index this document, it will automatically pull data from any model field (or property) with a matching name.
So in this example, ``title``, ``body``, and ``author`` will automatically be sent for indexing, but you will need to generate ``word_count``
yourself. To do this, you can implement a ``prepare_word_count`` class method::

    class PostDoc (seeker.ModelIndex):
        # ...

        @classmethod
        def prepare_word_count(cls, obj):
            return len(obj.body.split())

Alternatively, you could declare a ``word_count`` property on the ``Post`` model.


Customizing The Entire Data Mapping
-----------------------------------

If, for some reason, you need to customize the entire data mapping process, you may override the ``serialize`` class method::

    class PostDoc (seeker.ModelIndex):
        # ...

        @classmethod
        def serialize(cls, obj):
            # Let seeker grab the field values it knows about from the model.
            data = super(PostDoc, cls).serialize(obj)
            # Manipulate the data from the default implementation. Or not.
            return data

The default implementation of ``serialize`` calls :meth:`seeker.mapping.serialize_object` and ``get_id``.


What Gets Indexed and How
-------------------------

When re-indexing a mapping, the process is as follows:

    1. :meth:`seeker.mapping.ModelIndex.documents` is called, and expected to yield a single dictionary at a time to index.
    2. :meth:`seeker.mapping.ModelIndex.queryset` is called to get the queryset of Django objects to index.
    3. The resulting queryset is sliced into groups of ``batch_size`` (ordered by PK), to avoid a single large query.
    4. For each object, :meth:`seeker.mapping.ModelIndex.should_index` is called to determine if the object should be indexed. By default, all objects are indexed.
    5. :meth:`seeker.mapping.ModelIndex.get_id` and :meth:`seeker.mapping.ModelIndex.serialize` are called to generate the ID and data sent to Elasticsearch for each object.


Non-Django Documents
--------------------

It's possible to use seeker to build documents not associated with Django models. To do so, simply subclass
``seeker.Indexable`` instead of ``seeker.ModelIndex``, and override ``seeker.ModelIndex.documents``, like so::

    class OtherDoc (seeker.Indexable):

        @classmethod
        def documents(cls, **kwargs):
            return [
                {'name': 'Dan Watson', 'comment': 'Hello wife.'},
                {'name': 'Alexa Watson', 'comment': 'Hello husband.'},
            ]


Module Documentation
--------------------

.. automodule:: seeker.mapping
   :members:
   :exclude-members: type_map
