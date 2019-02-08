from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.encoding import python_2_unicode_compatible


@python_2_unicode_compatible
class SavedSearch(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='seeker_searches', on_delete=models.CASCADE)
    name = models.CharField(max_length=100, blank=True)
    url = models.CharField(max_length=200, db_index=True)
    querystring = models.TextField(blank=True)
    default = models.BooleanField(default=False)
    date_created = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        ordering = ('name',)
        verbose_name_plural = 'saved searches'

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        if self.querystring:
            return '%s?%s%s%s%d' % (self.url, self.querystring, ('&' if self.querystring else ''), 'saved_search=', self.pk)
        else:
            return self.url

    def get_details_dict(self):
        return { 'pk': self.pk, 'name': self.name, 'url': self.url, 'default': self.default }


@python_2_unicode_compatible
class AdvancedSavedSearch(SavedSearch):
    search_object = models.TextField()

    def __str__(self):
        return self.name

    def get_details_dict(self):
        details_dict = super(AdvancedSavedSearch, self).get_details_dict()
        details_dict.update({ 'search_object': self.search_object })
        return details_dict
