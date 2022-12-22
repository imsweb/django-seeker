from django.contrib import admin

from seeker.models import AdvancedSavedSearch, SavedSearch


class SavedSearchAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'url', 'querystring', 'default', 'date_created')
    list_filter = ('url', 'user', 'default')

class AdvancedSavedSearchAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'url', 'search_object', 'default', 'date_created')
    list_filter = ('url', 'user', 'default')


admin.site.register(SavedSearch, SavedSearchAdmin)
admin.site.register(AdvancedSavedSearch, AdvancedSavedSearchAdmin)
