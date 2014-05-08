Mappings
========

Our Example Model
-----------------

For the purposes of this document, take the following models::

    class Author (models.Model):
        name = models.CharField(max_length=100)
        
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

Basic Mappings
--------------

Mappings are what translates python objects into ElasticSearch data. The simplest mapping you can define
just takes a model class, and automatically indexes any field it can::

    import seeker
    from .models import Post
    
    class PostMapping (seeker.Mapping):
        model = Post

Most built-in Django field types are automatically indexed, including ``ForeignKey`` and ``ManyToManyField`` (using
their unicode representations). Foreign keys and many-to-many relationships are also marked as being facetable,
as are boolean fields.


Customizing Field Mappings
--------------------------

You can specify which model fields you want to index by setting a list of field names::

    class PostMapping (seeker.Mapping):
        model = Post
        fields = ('title', 'body', 'published')

You can also fully customize the schema by setting ``fields`` to a dictionary::

    class PostMapping (seeker.Mapping):
        model = Post
        fields = {
            'title': seeker.StringType(index=False),
            'body': seeker.StringType,
            'word_count': seeker.NumberType,
        }

When Seeker goes to index this mapping, it will still automatically pull data from any model field with a matching name.
So in this example, ``title`` and ``body`` will automatically be sent for indexing, but you will need to generate ``word_count``
yourself. To do this, you can override the ``get_data`` mapping method::

    class PostMapping (seeker.Mapping):
        ...

        def get_data(self, obj):
            data = super(PostMapping, self).get_data(obj) # Let Seeker grab the field values it knows about from the model
            data['word_count'] = len(data['body'].split())
            return data


Module Documentation
--------------------

.. autoclass:: seeker.mapping.Mapping
   :members:
   :exclude-members: type_map
