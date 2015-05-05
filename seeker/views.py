from django.views.generic import TemplateView
from django.shortcuts import redirect
from django.contrib import messages
from django.http import StreamingHttpResponse
from django.conf import settings
from elasticsearch.helpers import scan
from elasticsearch_dsl.connections import connections
import collections
import six
import re

class SeekerView (TemplateView):
    document = None
    """
    A :class:`elasticsearch_dsl.DocType` class to present a view for.
    """

    available = None
    """
    A list of available field names. If empty or ``None``, all mapping fields are available.
    """

    display = None
    """
    A list of field names to display by default. If empty or ``None``, all mapping fields are displayed.
    """

    search = None
    """
    A list of field names to search. By default, will included all fields defined on the document mapping.
    """

    highlight = True
    """
    A list of field names to highlight, or True/False to enable/disable highlighting for all fields.
    """

    template_name = 'seeker/seeker.html'
    """
    The template to render.
    """

    facets = []
    """
    A list of :class:`seeker.Facet` objects to facet the results by.
    """

    page_param = 'page'
    """
    The name of the paging GET parameter to use.
    """

    page_size = 10
    """
    The number of results to show per page.
    """

    links = None
    """
    A list of field names to display links for. If empty or ``None``, the first display field will be a link.
    """

    can_save = True
    """
    Whether searches for this view can be saved.
    """

    export_name = 'seeker'
    """
    The filename (without extension, which will be .csv) to use when exporting data from this view.
    """

    def _querystring(self):
        qs = self.request.META.get('QUERY_STRING', '')
        qs = re.sub(r'%s=\d+' % self.page_param, '', qs).replace('&&', '&')
        if qs.startswith('&'):
            qs = qs[1:]
        if qs.endswith('&'):
            qs = qs[:-1]
        return qs

    def clean_sort(self, fields):
        """
        Given a list of potential field names to sort by, returns a list of valid sort fields.
        """
        valid_fields = set()
        for f in self.document._doc_type.mapping:
            valid_fields.add(f)
            valid_fields.add('-%s' % f)
        return [f for f in fields if f in valid_fields]

    def get_facets(self):
        """
        Returns a list of :class:`seeker.Facet` objects to facet results by. Defaults to :attr:`self.facets`.
        """
        return self.facets

    def get_default_fields(self):
        """
        Returns a list of field names to display by default if the user has not specified any. Defaults to all
        mapping fields.
        """
        return self.display or [name for name in self.document._doc_type.mapping]

    def get_url(self, result, field_name):
        """
        Returns a URL for the specified result row and field name. By default, this simply returns the absolute URL of
        ``result.instance``, if it exists. This method can be overridden (in combination with :attr:`links`) to provide links
        for any number of columns based on field name.
        """
        try:
            return result.instance.get_absolute_url()
        except:
            return ''

    def get_search(self, keywords=None, facets=None, aggregate=True):
        s = self.document.search()
        if keywords:
            s = s.query('query_string', query=keywords, analyzer='snowball', fields=self.search, auto_generate_phrase_queries=True,
                    default_operator=getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'AND'))
        if facets:
            for facet in facets:
                if facet.field in self.request.GET:
                    s = facet.filter(s, self.request.GET.getlist(facet.field))
                if aggregate:
                    facet.apply(s)
        return s

    def get_context_data(self, **kwargs):
        """
        Returns the context for rendering :attr:`template_name`. The available context variables are:

            results
                An :class:`elasticsearch_dsl.result.Response` instance that can be iterated.
            facets
                A list of :class:`seeker.Facet` objects used to facet results.
            display_fields
                A list of mapping field names to display.
            facet_fields
                A list of facet field names that are currently filtered.
            link_fields
                A list of field names that should be displayed as links.
            keywords
                The keywords string.
            request_path
                The current request path, with no querystring.
            page
                The current page number.
            page_param
                The name of the paging GET parameter.
            page_size
                The number of results to show on a page.
            querystring
                The querystring for this request.
            can_save
                Whether this search can be saved.
            current_search
                The current SavedSearch object, or ``None``.
            saved_searches
                A list of all SavedSearch objects for this view.
            document
                An :class:`elasticsearch_dsl.DocType` class.
            field_labels
                A list of tuples (field_name, label) for each field defined in the document mapping.
        """
        keywords = self.request.GET.get('q', '').strip()
        available_fields = self.available or list(sorted(self.document._doc_type.mapping))
        display_fields = self.request.GET.getlist('d') or self.get_default_fields()
        page = self.request.GET.get(self.page_param, '').strip()
        page = int(page) if page.isdigit() else 1
        sort_fields = self.clean_sort(self.request.GET.getlist('sort'))
        offset = (page - 1) * self.page_size
        
        # Build an OrderedDict of Facet -> [selected values]
        facets = collections.OrderedDict((f, self.request.GET.getlist(f.field)) for f in self.get_facets())
        
        search = self.get_search(keywords, facets=facets).sort(*sort_fields)[offset:offset + self.page_size]
        if self.highlight:
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else display_fields
            search = search.highlight(*highlight_fields)

        querystring = self._querystring()
        if self.request.user and self.request.user.is_authenticated():
            current_search = self.request.user.seeker_searches.filter(url=self.request.path, querystring=querystring).first()
            saved_searches = self.request.user.seeker_searches.filter(url=self.request.path)
        else:
            current_search = None
            saved_searches = []

        params = super(SeekerView, self).get_context_data(**kwargs)
        params.update({
            'results': search.execute(),
            'facets': facets,
            'display_fields': display_fields,
            'link_fields': self.links or (display_fields[0],),
            'keywords': keywords,
            'request_path': self.request.path,
            'page': page,
            'page_param': self.page_param,
            'page_size': self.page_size,
            'querystring': querystring,
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated(),
            'current_search': current_search,
            'saved_searches': saved_searches,
            'document': self.document,
            'field_labels': [(name, self.document.label_for_field(name)) for name in available_fields],
        })
        return params

    def export(self, request):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        keywords = self.request.GET.get('q', '').strip()
        display_fields = self.request.GET.getlist('d') or self.get_default_fields()
        search = self.get_search(keywords, aggregate=False)

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(six.text_type(v) for v in value)
            return '"%s"' % six.text_type(value).replace('"', '""')

        def csv_generator():
            yield ','.join(self.document.label_for_field(f) for f in display_fields) + '\n'
            for result in scan(connections.get_connection('default'), query=search.to_dict(), index=self.document._doc_type.index, doc_type=self.document._doc_type.name):
                yield ','.join(csv_escape(result['_source'].get(f, '')) for f in display_fields) + '\n'

        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename=%s.csv' % self.export_name
        return resp

    def get(self, request, *args, **kwargs):
        """
        Overridden from Django's TemplateView for two purposes:

            1. Check to see if ``_export`` is present in ``request.GET``, and if so, defer handling to ``self.export``.
            2. If there is no querystring, check to see if there is a default SavedSearch set for this view, and if so, redirect to it.
        """
        if '_export' in request.GET:
            return self.export(request)
        try:
            querystring = self._querystring()
            if not querystring:
                default = request.user.seeker_searches.get(url=request.path, default=True)
                if default.querystring != querystring:
                    return redirect(default)
        except:
            pass
        return super(SeekerView, self).get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Overridden from Django's TemplateView to handle saved search actions.
        """
        if not self.can_save:
            return redirect(request.get_full_path())
        qs = request.POST.get('querystring', '').strip()
        if '_save' in request.POST:
            name = request.POST.get('name', '').strip()
            if not name:
                messages.error(request, 'You did not provide a name for the saved search. Please try again.')
                return redirect(request.get_full_path())
            default = request.POST.get('default', '').strip() == '1'
            if default:
                request.user.seeker_searches.filter(url=request.path).update(default=False)
            search = request.user.seeker_searches.create(name=name, url=request.path, querystring=qs, default=default)
            messages.success(request, 'Successfully saved "%s".' % search)
            return redirect(search)
        elif '_default' in request.POST:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
            request.user.seeker_searches.filter(url=request.path, querystring=qs).update(default=True)
        elif '_unset' in request.POST:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
        elif '_delete' in request.POST:
            request.user.seeker_searches.filter(url=request.path, querystring=qs).delete()
        return redirect(request.get_full_path())
