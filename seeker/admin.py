from django.contrib import admin

try:
    from .models import SavedSearch
    
    class SavedSearchAdmin (admin.ModelAdmin):
        list_display = ('name', 'user', 'url', 'querystring', 'default', 'date_created')
        list_filter = ('url', 'user', 'default')
    
    admin.site.register(SavedSearch, SavedSearchAdmin)
except ImportError:
    pass
