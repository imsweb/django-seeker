from django.views.generic import TemplateView
from django.shortcuts import redirect
from django.contrib import messages
from django.http import StreamingHttpResponse, Http404
from django.utils.encoding import force_text
from .query import TermAggregate
from .utils import get_facet_filters
from .mapping import StringType, ObjectType

from elasticsearch.helpers import scan

from six.moves.urllib.parse import parse_qsl, urlencode


class SeekerView (TemplateView):
    mapping = None
    """
    A :class:`seeker.mapping.Mapping` class to present a view for.
    """

    template_name = 'seeker/seeker.html'
    """
    The template to render.
    """

    page_param = 'page'
    """
    The name of the paging GET parameter to use.
    """

    page_size = 10
    """
    The number of results to show per page.
    """

    display = None
    """
    A list of field names to display by default. If empty or ``None``, all mapping fields are displayed.
    """

    links = None
    """
    A list of field names to display links for. If empty or ``None``, the first display field will be a link.
    """

    sort = None
    """
    The default field to sort on. If empty or ``None``, results will be sorted by relevance when no other sort is supplied.
    """

    sort_overrides = None
    """
    A dictionary of field names mapped to the respective sort field to be used when sorting.
    """

    can_save = True
    """
    Whether searches for this view can be saved.
    """

    export_name = 'seeker'
    """
    The filename (without extension, which will be .csv) to use when exporting data from this view.
    """

    permission = None
    """
    If specified, a permission to check (using ``request.user.has_perm``) for this view.
    """

    def _querystring(self):
        qs = self.request.META.get('QUERY_STRING', '')
        if qs.startswith('&'):
            qs = qs[1:]
        if qs.endswith('&'):
            qs = qs[:-1]
        initial_qs_parts = [part for part in parse_qsl(qs, keep_blank_values=True)]
        saved_search = [part for part in initial_qs_parts if part[0] == 'saved_search' and part[1]]
        qs_parts = [part for part in initial_qs_parts if part[0] not in (self.page_param, 'saved_search')]
        # If 1) we're not viewing a saved search, 2) a default sort is specified for this view, and 3) a sort isn't supplied in the querystring,
        # add the default sort to the querystring in order to have the sort direction displayed properly in the display fields header.
        if (not saved_search or len(saved_search) > 1) and self.sort:
            sort = [part for part in qs_parts if part[0] == 'sort' and part[1]]
            if not sort:
                qs_parts.append(('sort', self.sort))
        qs = urlencode(qs_parts)
        return qs

    def get_facets(self):
        """
        Yields :class:`seeker.query.Aggregate` objects to facet on. By default, yields a :class:`seeker.query.TermAggregate`
        instance for any mapping fields with ``facet=True``.
        """
        mapping = self.mapping.instance()
        for name, t in mapping.field_map.items():
            if isinstance(t, StringType) and t.facet:
                field_name = name + '.raw' if t.index else name
                yield TermAggregate(field_name, label=mapping.field_label(name))

    def get_default_fields(self):
        """
        Returns a list of field names to display by default if the user has not specified any. Defaults to all
        mapping fields.
        """
        if self.display:
            return self.display
        mapping = self.mapping.instance()
        return list(mapping.field_map.keys())

    def get_selectable_fields(self):
        """
        Returns a dictionary of selectable fields (those that can be selected for display) with field names mapped to
        their associated labels. Defaults to all mapping fields by way of a pass-through to ``mapping.field_labels``.
        """
        mapping = self.mapping.instance()
        return mapping.field_labels

    def get_url(self, result, field_name):
        """
        Returns a URL for the specified result row and field name. By default, this simply returns the absolute URL of
        ``result.instance``, if it exists. This method can be overridden (in combination with :attr:`links`) to provide links
        for any number of columns based on field name.
        """
        try:
            return result.instance.get_absolute_url()
        except Exception:
            return ''

    def get_query(self):
        """
        Returns a query to pass to ``mapping.query``. Defaults to the "q" GET parameter, but can be overridden to provide
        a query dict that will be serialized to Elasticsearch.
        """
        return self.request.GET.get('q', '').strip()

    def get_sort(self):
        """
        Returns the field to sort on. Defaults to the "sort" GET parameter with a fallback to the default sort for this view.
        """
        return self.request.GET.get('sort', self.sort)

    def get_display_fields(self):
        """
        Returns a list of field names to display.
        """
        return self.request.GET.getlist('d') or self.get_default_fields()

    def get_saved_search(self):
        """
        Returns the "saved_search" GET parameter if it's in the proper format, otherwise returns None.
        """
        saved_search_vals = [val for val in self.request.GET.getlist('saved_search') if val]
        if len(saved_search_vals) == 1 and saved_search_vals[0].isdigit():
            return saved_search_vals[0]
        return None

    def extra_filters(self):
        """
        Returns a list of seeker.query.F objects that should be included as filters.
        """
        return []

    def get_context_data(self, **kwargs):
        """
        Returns the context for rendering :attr:`template_name`. The available context variables are:

            results
                A :class:`seeker.query.ResultSet` instance.
            filters
                A dictionary of field name filters.
            display_fields
                A list of mapping field names to display.
            link_fields
                A list of field names that should be displayed as links.
            selectable_fields
                A dictionary of selectable fields (those that can be selected for display) with field names mapped to their associated labels.
            sort
                The field to sort on.
            sort_overrides
                A dictionary of field names mapped to the respective sort field to be used when sorting.
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
            saved_search
                The current SavedSearch object, or ``None``.
            saved_searches
                A list of all SavedSearch objects for this view.
            mapping
                An instance of :attr:`mapping`.
        """
        mapping = self.mapping.instance()
        keywords = self.get_query()
        display_fields = self.get_display_fields()
        page = self.request.GET.get(self.page_param, '').strip()
        page = int(page) if page.isdigit() else 1
        sort = self.get_sort()

        facets = list(self.get_facets())
        filters, facet_filters = get_facet_filters(self.request.GET, facets)

        offset = (page - 1) * self.page_size

        highlight = []
        for name in display_fields:
            try:
                f = mapping.field_map[name]
                if isinstance(f, ObjectType):
                    highlight.append('%s.*' % name)
                else:
                    highlight.append(name)
            except Exception:
                pass

        facet_filters.extend(self.extra_filters())
        results = mapping.query(
            query=keywords,
            filters=facet_filters,
            facets=facets,
            highlight=highlight,
            limit=self.page_size,
            offset=offset,
            sort=sort)

        querystring = self._querystring()
        saved_search = None
        if self.request.user and self.request.user.is_authenticated():
            saved_search_pk = self.get_saved_search()
            if saved_search_pk:
                from .models import SavedSearch
                try:
                    saved_search = self.request.user.seeker_searches.get(pk=saved_search_pk, url=self.request.path, querystring=querystring)
                except SavedSearch.DoesNotExist:
                    pass
            saved_searches = self.request.user.seeker_searches.filter(url=self.request.path)
        else:
            saved_searches = []

        params = super(SeekerView, self).get_context_data(**kwargs)
        params.update({
            'results': results,
            'filters': filters,
            'display_fields': display_fields,
            'link_fields': self.links or (display_fields[0],),
            'selectable_fields': self.get_selectable_fields(),
            'sort': sort,
            'sort_overrides': self.sort_overrides,
            'keywords': keywords,
            'request_path': self.request.path,
            'page': page,
            'page_param': self.page_param,
            'page_size': self.page_size,
            'querystring': querystring,
            'can_save': self.can_save,
            'saved_search': saved_search,
            'saved_searches': saved_searches,
            'mapping': mapping,
        })
        return params

    def export(self, request):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        mapping = self.mapping.instance()
        display_fields = self.get_display_fields()
        sort = self.get_sort()
        facet_filters = get_facet_filters(self.request.GET, self.get_facets())[1]
        facet_filters.extend(self.extra_filters())
        query = mapping.query(query=self.get_query(), filters=facet_filters, sort=sort).to_elastic()

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(str(v) for v in value)
            return '"%s"' % str(value).replace('"', '""')

        def csv_generator():
            yield ','.join(force_text(mapping.field_label(f)) for f in display_fields) + '\n'
            for result in scan(mapping.es, index=mapping.index_name, doc_type=mapping.doc_type, query=query):
                yield ','.join(csv_escape(result['_source'].get(f, '')) for f in display_fields) + '\n'

        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename=%s.csv' % self.export_name
        return resp

    def check_permission(self, request):
        """
        Check to see if the user has permission for this view. This method may optionally return an ``HttpResponse``.
        """
        if self.permission and not request.user.has_perm(self.permission):
            raise Http404

    def dispatch(self, request, *args, **kwargs):
        """
        Overridden to perform permission checking by calling ``self.check_permission``.
        """
        resp = self.check_permission(request)
        if resp:
            return resp
        return super(SeekerView, self).dispatch(request, *args, **kwargs)

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
        except Exception:
            pass
        return super(SeekerView, self).get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Overridden from Django's TemplateView to handle saved search actions.
        """
        if not self.can_save:
            return redirect(request.get_full_path())
        qs = request.POST.get('querystring', '').strip()
        saved_search_pk = request.POST.get('saved_search', '').strip()
        if not saved_search_pk.isdigit():
            saved_search_pk = None
        if '_save' in request.POST:
            name = request.POST.get('name', '').strip()
            if not name:
                messages.error(request, 'You did not provide a name for the saved search. Please try again.')
                return redirect(request.get_full_path())
            default = request.POST.get('default', '').strip() == '1'
            if default:
                request.user.seeker_searches.filter(url=request.path).update(default=False)
            search_values = {'url': request.path, 'querystring': qs, 'default': default}
            search, created = request.user.seeker_searches.update_or_create(name=name, defaults=search_values)
            messages.success(request, 'Successfully saved "%s".' % search)
            return redirect(search)
        elif '_default' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
            request.user.seeker_searches.filter(pk=saved_search_pk, url=request.path, querystring=qs).update(default=True)
        elif '_unset' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
        elif '_delete' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(pk=saved_search_pk, url=request.path, querystring=qs).delete()
            return redirect(request.path)
        return redirect(request.get_full_path())
