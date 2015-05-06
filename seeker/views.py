from django.views.generic import TemplateView
from django.shortcuts import redirect
from django.contrib import messages
from django.http import StreamingHttpResponse, QueryDict
from django.template import loader, Context
from django.utils.html import escape
from django.conf import settings
from elasticsearch.helpers import scan
from elasticsearch_dsl.connections import connections
import elasticsearch_dsl as dsl
import collections
import six
import re

class Column (object):

    def __init__(self, field, label=None, sort=None, template=None):
        self.field = field
        self.label = label or field.replace('_', ' ').replace('.raw', '').capitalize()
        self.sort = sort
        self.template = template

    def __str__(self):
        return self.label

    def __repr__(self):
        return 'Column(%s)' % self.field

    def header(self, querystring):
        if not self.sort:
            return escape(self.label)
        q = QueryDict(querystring, mutable=True)
        field = q.get('s', '')
        direction = 'asc'
        if field.startswith('-'):
            field = field[1:]
            direction = 'desc'
        d = '' if direction == 'desc' or field != self.field else '-'
        q['s'] = '%s%s' % (d, self.field)
        return '<a href="?%s" class="sort %s">%s</a>' % (q.urlencode(), direction, escape(self.label))

    def context(self, result, **kwargs):
        return kwargs

    def render(self, result, **kwargs):
        try:
            value = result.meta.highlight[self.field][0]
        except:
            value = getattr(result, self.field, None)
        search_templates = [
            'seeker/%s/%s.html' % (result.meta.doc_type, self.field),
        ]
        if self.template:
            search_templates.insert(0, self.template)
        try:
            t = loader.select_template(search_templates)
            params = {
                'result': result,
                'field': self.field,
                'value': value,
            }
            params.update(self.context(result, **kwargs))
            return t.render(Context(params))
        except:
            return value

    def export_value(self, source):
        return source.get(self.field, '')

class SeekerView (TemplateView):
    document = None
    """
    A :class:`elasticsearch_dsl.DocType` class to present a view for.
    """

    columns = None
    """
    A list of Column objects, or strings representing mapping field names. If None, all mapping fields will be available.
    """

    display = None
    """
    A list of field/column names to display by default.
    """

    sort = None
    """
    A list of field/column names to sort by default, or None for no default sort order.
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

    facets = None
    """
    A list of :class:`seeker.Facet` objects to facet the results by.
    """

    page_size = 10
    """
    The number of results to show per page.
    """

    can_save = True
    """
    Whether searches for this view can be saved.
    """

    export_name = 'seeker'
    """
    The filename (without extension, which will be .csv) to use when exporting data from this view.
    """

    show_rank = True
    """
    Whether or not to show a Rank column when performing keyword searches.
    """

    field_labels = {}
    """
    A dictionary of field label overrides.
    """

    sort_fields = {}
    """
    A dictionary of sort field overrides.
    """

    def _querystring(self, remove_sort=False):
        qs = self.request.META.get('QUERY_STRING', '')
        qs = re.sub(r'p=\d+', '', qs).replace('&&', '&')
        if remove_sort:
            qs = re.sub(r's=[^&$]+', '', qs).replace('&&', '&')
        return qs.strip('&')

    def get_field_label(self, field_name):
        """
        Given a field name, returns a human readable label for the field.
        """
        if field_name.endswith('.raw'):
            field_name = field_name[:-4]
        if field_name in self.field_labels:
            return self.field_labels[field_name]
        try:
            # If the document is a ModelIndex, try to get the verbose_name of the Django field.
            f = self.document.queryset().model._meta.get_field(field_name)
            return f.verbose_name.capitalize()
        except:
            # Otherwise, just make the field name more human-readable.
            return field_name.replace('_', ' ').capitalize()

    def get_field_sort(self, field_name):
        """
        Given a field name, returns the field name that should be used for sorting. If a mapping defines
        a .raw sub-field, that is used, otherwise the field name itself is used if index=not_analyzed.
        """
        if field_name.endswith('.raw'):
            return field_name
        if field_name in self.sort_fields:
            return self.sort_fields[field_name]
        if field_name in self.document._doc_type.mapping:
            dsl_field = self.document._doc_type.mapping[field_name]
            if 'raw' in dsl_field.fields:
                return '%s.raw' % field_name
            elif getattr(dsl_field, 'index', None) == 'not_analyzed':
                return field_name
        return None

    def get_columns(self):
        """
        Returns a list of :class:`seeker.Column` objects based on self.columns, converting any strings.
        """
        columns = []
        if not self.columns:
            # If not specified, all mapping fields will be available.
            for f in sorted(self.document._doc_type.mapping):
                label = self.get_field_label(f)
                sort = self.get_field_sort(f)
                columns.append(Column(f, label=label, sort=sort))
        else:
            # Otherwise, go through and convert any strings to Columns.
            for c in self.columns:
                if isinstance(c, six.string_types):
                    label = self.get_field_label(c)
                    sort = self.get_field_sort(c)
                    columns.append(Column(c, label=label, sort=sort))
                elif isinstance(c, Column):
                    columns.append(c)
        return columns

    def get_facets(self):
        """
        Returns a list of :class:`seeker.Facet` objects to facet results by. Defaults to :attr:`self.facets`.
        """
        return self.facets or []

    def get_search(self, keywords=None, facets=None, aggregate=True):
        s = self.document.search(track_scores=True)
        if keywords:
            s = s.query('query_string', query=keywords, analyzer='snowball', fields=self.search, auto_generate_phrase_queries=True,
                    default_operator=getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'AND'))
        if facets:
            for facet in facets:
                if facet.field in self.request.GET:
                    # TODO: pass request.GET directly?
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
            columns
                A list of available Column objects.
            display_columns
                A list of Column objects to display.
            keywords
                The keywords string.
            request_path
                The current request path, with no querystring.
            page
                The current page number.
            page_size
                The number of results to show on a page.
            querystring
                The querystring for this request (without page).
            reset_querystring
                The querystring for this request, without page and sort params.
            can_save
                A boolean indicating whether this search can be saved.
            current_search
                The current SavedSearch object, or ``None``.
            saved_searches
                A list of all SavedSearch objects for this view.
            show_rank
                A boolean indicating whether to show the Rank column.
            document
                An :class:`elasticsearch_dsl.DocType` class.
            export_name
                The filename of the export file (without .csv), if available.
        """
        keywords = self.request.GET.get('q', '').strip()

        # Get all possible columns, then figure out which should be displayed.
        columns = self.get_columns()
        display_fields = self.request.GET.getlist('d') or self.display
        display_columns = [c for c in columns if not display_fields or c.field in display_fields]

        # Calculate paging information.
        page = self.request.GET.get('p', '').strip()
        page = int(page) if page.isdigit() else 1
        offset = (page - 1) * self.page_size

        # Make sure we sanitize the sort fields.
        sort_fields = []
        column_lookup = {c.field: c for c in columns}
        sorts = self.request.GET.getlist('s') or self.sort or []
        for s in sorts:
            # Get the column based on the field name, and use it's "sort" field, if applicable.
            c = column_lookup.get(s.replace('-', ''), None)
            if c and c.sort:
                sort_fields.append('-%s' % c.sort if s.startswith('-') else c.sort)

        # Build an OrderedDict of Facet -> [selected values]
        facets = collections.OrderedDict((f, self.request.GET.getlist(f.field)) for f in self.get_facets())

        # Build the elasticsearch_dsl.Search object.
        search = self.get_search(keywords, facets=facets).sort(*sort_fields)[offset:offset + self.page_size]

        # Highlight fields.
        if self.highlight:
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.field for c in display_columns if c.field]
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
            'columns': columns,
            'display_columns': display_columns,
            'keywords': keywords,
            'request_path': self.request.path,
            'page': page,
            'page_size': self.page_size,
            'querystring': querystring,
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated(),
            'current_search': current_search,
            'saved_searches': saved_searches,
            'document': self.document,
            'show_rank': self.show_rank,
            'reset_querystring': self._querystring(remove_sort=True),
            'export_name': self.export_name,
        })
        return params

    def export(self, request):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        keywords = self.request.GET.get('q', '').strip()

        # Get all possible columns, then figure out which should be displayed.
        display_fields = self.request.GET.getlist('d') or self.display
        display_columns = [c for c in self.get_columns() if not display_fields or c.field in display_fields]

        search = self.get_search(keywords, aggregate=False)

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(six.text_type(v) for v in value)
            return '"%s"' % six.text_type(value).replace('"', '""')

        def csv_generator():
            yield ','.join(c.label for c in display_columns) + '\n'
            for result in scan(connections.get_connection('default'), query=search.to_dict(), index=self.document._doc_type.index, doc_type=self.document._doc_type.name):
                yield ','.join(csv_escape(c.export_value(result['_source'])) for c in display_columns) + '\n'

        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename=%s.csv' % self.export_name
        return resp

    def get(self, request, *args, **kwargs):
        """
        Overridden from Django's TemplateView for two purposes:

            1. Check to see if ``_export`` is present in ``request.GET``, and if so, defer handling to ``self.export``.
            2. If there is no querystring, check to see if there is a default SavedSearch set for this view, and if so, redirect to it.
        """
        if self.export_name and '_export' in request.GET:
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
        qs = self._querystring()
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
