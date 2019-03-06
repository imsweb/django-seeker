import six
from django.db import models


@six.python_2_unicode_compatible
class Author (models.Model):
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=100)
    bio = models.TextField()

    def __str__(self):
        return '%s %s' % (self.first_name, self.last_name)


@six.python_2_unicode_compatible
class Category (models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


@six.python_2_unicode_compatible
class Book (models.Model):
    title = models.CharField(max_length=200)
    authors = models.ManyToManyField(Author, related_name='books')
    category = models.ForeignKey(Category, related_name='books', on_delete=models.CASCADE)
    date_published = models.DateField()
    pages = models.IntegerField()
    in_print = models.BooleanField(default=True)

    def __str__(self):
        return self.title


@six.python_2_unicode_compatible
class Magazine (models.Model):
    name = models.CharField(max_length=200)
    issue_date = models.DateField()

    def __str__(self):
        return self.name
