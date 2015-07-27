from django.conf import settings
from django.contrib import messages
from django.http import JsonResponse, StreamingHttpResponse, QueryDict, Http404
from django.shortcuts import render, redirect
from django.template import loader, Context
from django.utils.encoding import force_text
from django.utils.html import escape
from django.views.generic import View
from elasticsearch.helpers import scan
from elasticsearch_dsl.connections import connections
import collections
import elasticsearch_dsl as dsl
import re
import six
import urllib

class Column (object):

    def __init__(self, field, label=None, sort=None, value_format=None, template=None):
        self.field = field
        self.label = label or field.replace('_', ' ').replace('.raw', '').capitalize()
        self.sort = sort
        self.template = template
        self.value_format = value_format

    def __str__(self):
        return self.label

    def __repr__(self):
        return 'Column(%s)' % self.field

    def header(self, querystring):
        if not self.sort:
            return '<th>%s</th>' % escape(self.label)
        q = QueryDict(querystring, mutable=True)
        field = q.get('s', '')
        cls = 'sort'
        if field.lstrip('-') == self.field:
            # If the current sort field is this field, give it a class a change direction.
            cls += ' desc' if field.startswith('-') else ' asc'
            d = '' if field.startswith('-') else '-'
            q['s'] = '%s%s' % (d, self.field)
        else:
            q['s'] = self.field
        return '<th class="%s"><a href="?%s" data-sort="%s">%s</a></th>' % (cls, q.urlencode(), q['s'], escape(self.label))

    def context(self, result, **kwargs):
        return kwargs

    def render(self, result, **kwargs):
        value = getattr(result, self.field, None)
        if self.value_format:
            value = self.value_format(value)
        try:
            highlight = result.meta.highlight[self.field]
        except:
            highlight = []
        search_templates = [
            'seeker/%s/%s.html' % (result.meta.doc_type, self.field),
            'seeker/column.html',
        ]
        if self.template:
            search_templates.insert(0, self.template)
        t = loader.select_template(search_templates)
        params = {
            'result': result,
            'field': self.field,
            'value': value,
            'highlight': highlight,
        }
        params.update(self.context(result, **kwargs))
        return t.render(Context(params))

    def export_value(self, result):
        return getattr(result, self.field, '')

class SeekerView (View):
    document = None
    """
    A :class:`elasticsearch_dsl.DocType` class to present a view for.
    """

    template_name = 'seeker/seeker.html'
    """
    The overall seeker template to render.
    """

    results_template = 'seeker/results.html'
    """
    The template used to render the search results.
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

    facets = []
    """
    A list of :class:`seeker.Facet` objects that are available to facet the results by.
    """

    initial_facets = {}
    """
    A list of facet fields to show by default, or a dictionary of facet field to list of initial values.
    """

    page_size = 10
    """
    The number of results to show per page.
    """

    page_spread = 7
    """
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

    operator = getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'AND')
    """
    """

    def normalized_querystring(self, qs=None, ignore=None):
        data = QueryDict(qs) if qs is not None else self.request.GET
        parts = []
        for key in sorted(data):
            if ignore and key in ignore:
                continue
            values = data.getlist(key)
            # Make sure display/facet/sort fields maintain their order. Everything else can be sorted alphabetically for consistency.
            if key not in ('d', 'f', 's'):
                values = sorted(values)
            parts.extend(urllib.urlencode({key: val}) for val in values)
        return '&'.join(parts)

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
            if not isinstance(dsl_field, dsl.String):
                return field_name
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
            display_sort = lambda name: self.display.index(name) if self.display and name in self.display else 9999
            for f in sorted(self.document._doc_type.mapping, key=display_sort):
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

    def get_keywords(self):
        return self.request.GET.get('q', '').strip()

    def get_facets(self, initial=None, exclude=None):
        if initial is None:
            initial = {}
        facets = collections.OrderedDict()
        for f in self.facets:
            if f.field != exclude:
                facets[f] = self.request.GET.getlist(f.field) or initial.get(f.field, [])
        return facets

    def get_search_fields(self, mapping=None, prefix=''):
        if self.search:
            return self.search
        elif mapping is not None:
            fields = []
            for field_name in mapping:
                if mapping[field_name].to_dict().get('analyzer') == 'snowball':
                    fields.append(prefix + field_name)
                if hasattr(mapping[field_name], 'properties'):
                    fields.extend(self.get_search_fields(mapping=mapping[field_name].properties, prefix=prefix + field_name + '.'))
            return fields
        else:
            return self.get_search_fields(mapping=self.document._doc_type.mapping)

    def get_search(self, keywords=None, facets=None, aggregate=True):
        s = self.document.search().extra(track_scores=True)
        if keywords:
            s = s.query('query_string', query=keywords, analyzer='snowball', fields=self.get_search_fields(),
                auto_generate_phrase_queries=True, default_operator=self.operator)
        if facets:
            for facet, values in facets.items():
                if values:
                    s = facet.filter(s, values)
                if aggregate:
                    facet.apply(s)
        return s

    def render(self, initial=False):
        keywords = self.get_keywords()
        facets = self.get_facets(initial=self.initial_facets if initial else None)
        search = self.get_search(keywords, facets)

        # Get all possible columns, then figure out which should be displayed.
        columns = self.get_columns()
        display_fields = self.request.GET.getlist('d') or self.display
        display_columns = [c for c in columns if not display_fields or c.field in display_fields]

        # Make sure we sanitize the sort fields.
        sort_fields = []
        column_lookup = {c.field: c for c in columns}
        sorts = self.request.GET.getlist('s') or self.sort or []
        for s in sorts:
            # Get the column based on the field name, and use it's "sort" field, if applicable.
            c = column_lookup.get(s.lstrip('-'))
            if c and c.sort:
                sort_fields.append('-%s' % c.sort if s.startswith('-') else c.sort)

        # Highlight fields.
        if self.highlight:
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.field for c in display_columns if c.field]
            search = search.highlight(*highlight_fields)

        # Calculate paging information.
        page = self.request.GET.get('p', '').strip()
        page = int(page) if page.isdigit() else 1
        offset = (page - 1) * self.page_size

        # Finally, grab the results.
        results = search.sort(*sort_fields)[offset:offset + self.page_size].execute()

        context = {
            'document': self.document,
            'keywords': keywords,
            'columns': columns,
            'display_columns': display_columns,
            'facets': facets,
            'selected_facets': self.request.GET.getlist('f') or self.initial_facets.keys(),
            'form_action': self.request.path,
            'results': results,
            'page': page,
            'page_size': self.page_size,
            'page_spread': self.page_spread,
            'sort': sorts[0] if sorts else '',
            'querystring': self.normalized_querystring(ignore=['p']),
            'reset_querystring': self.normalized_querystring(ignore=['p', 's']),
            'show_rank': self.show_rank,
            'export_name': self.export_name,
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated(),
            'results_template': self.results_template,
        }

        if initial:
            return render(self.request, self.template_name, context)
        else:
            return JsonResponse({
                'table_html': loader.render_to_string(self.results_template, context),
                'facet_data': {facet.field: facet.data(results) for facet in self.facets},
            })

    def render_facet_query(self):
        keywords = self.get_keywords()
        facet = {f.field: f for f in self.facets}.get(self.request.GET.get('_facet'))
        if not facet:
            raise Http404()
        # We want to apply all the other facet filters besides the one we're querying.
        facet_values = self.get_facets(exclude=facet)
        search = self.get_search(keywords, facet_values, aggregate=False)
        fq = '.*' + self.request.GET.get('_query', '').strip() + '.*'
        facet.apply(search, include={'pattern': fq, 'flags': 'CASE_INSENSITIVE'})
        return JsonResponse(facet.data(search.execute()))

    def export(self):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        keywords = self.get_keywords()
        facets = self.get_facets()
        search = self.get_search(keywords, facets, aggregate=False)

        columns = self.get_columns()
        display_fields = self.request.GET.getlist('d') or self.display
        display_columns = [c for c in columns if not display_fields or c.field in display_fields]

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(force_text(v) for v in value)
            return '"%s"' % force_text(value).replace('"', '""')

        def csv_generator():
            yield ','.join('"%s"' % c.label for c in display_columns) + '\n'
            for result in search.scan():
                yield ','.join(csv_escape(c.export_value(result)) for c in display_columns) + '\n'

        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv; charset=utf-8')
        resp['Content-Disposition'] = 'attachment; filename=%s.csv' % self.export_name
        return resp

    def get(self, request, *args, **kwargs):
        if '_facet' in request.GET:
            return self.render_facet_query()
        elif '_export' in request.GET:
            return self.export()
        else:
            return self.render(initial=not request.is_ajax())
