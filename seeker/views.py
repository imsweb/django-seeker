from django.conf import settings
from django.contrib import messages
from django.http import JsonResponse, StreamingHttpResponse, QueryDict, Http404
from django.shortcuts import render, redirect
from django.template import loader, Context
from django.utils.encoding import force_text
from django.utils.html import escape
from django.utils.safestring import mark_safe
from django.views.generic import View
from seeker.templatetags.seeker import seeker_format
import collections
import elasticsearch_dsl as dsl
import six
import urllib
import re

class Column (object):
    view = None
    visible = False

    def __init__(self, field, label=None, sort=None, value_format=None, template=None, header=None, export=True, highlight=None):
        self.field = field
        self.label = label if label is not None else field.replace('_', ' ').replace('.raw', '').capitalize()
        self.sort = sort
        self.template = template
        self.value_format = value_format
        self.header_html = escape(self.label) if header is None else header
        self.export = export
        self.highlight = highlight

    def __str__(self):
        return self.label

    def __repr__(self):
        return 'Column(%s)' % self.field

    def bind(self, view, visible):
        self.view = view
        self.visible = visible
        search_templates = [
            'seeker/%s/%s.html' % (view.document._doc_type.name, self.field),
            'seeker/column.html',
        ]
        if self.template:
            search_templates.insert(0, self.template)
        self.template = loader.select_template(search_templates)
        return self

    def header(self):
        cls = '%s_%s' % (self.view.document._doc_type.name, self.field.replace('.', '_'))
        if not self.sort:
            return mark_safe('<th class="%s">%s</th>' % (cls, self.header_html))
        q = self.view.request.GET.copy()
        field = q.get('s', '')
        cls += ' sort'
        if field.lstrip('-') == self.field:
            # If the current sort field is this field, give it a class a change direction.
            cls += ' desc' if field.startswith('-') else ' asc'
            d = '' if field.startswith('-') else '-'
            q['s'] = '%s%s' % (d, self.field)
        else:
            q['s'] = self.field
        html = '<th class="%s"><a href="?%s" data-sort="%s">%s</a></th>' % (cls, q.urlencode(), q['s'], self.header_html)
        return mark_safe(html)

    def context(self, result, **kwargs):
        return kwargs

    def render(self, result, **kwargs):
        value = getattr(result, self.field, None)
        if self.value_format:
            value = self.value_format(value)
        try:
            if '*' in self.highlight:
                # If highlighting was requested for multiple fields, grab any matching fields as a dictionary.
                r = self.highlight.replace('*', r'\w+').replace('.', r'\.')
                highlight = {f: result.meta.highlight[f] for f in result.meta.highlight if re.match(r, f)}
            else:
                highlight = result.meta.highlight[self.highlight]
        except:
            highlight = []
        params = {
            'result': result,
            'field': self.field,
            'value': value,
            'highlight': highlight,
            'view': self.view,
            'user': self.view.request.user,
            'query': self.view.get_keywords(),
        }
        params.update(self.context(result, **kwargs))
        return self.template.render(Context(params))

    def export_value(self, result):
        export_field = self.field if self.export is True else self.export
        return seeker_format(getattr(result, export_field, ''))

class SeekerView (View):
    document = None
    """
    A :class:`elasticsearch_dsl.DocType` class to present a view for.
    """

    using = None
    """
    The ES connection alias to use.
    """

    index = None
    """
    The ES index to use. Defaults to the SEEKER_INDEX setting.
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

    exclude = None
    """
    A list of field names to exclude when generating columns.
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
    A dictionary of initial facets, mapping fields to lists of initial values.
    """

    page_size = 10
    """
    The number of results to show per page.
    """

    page_spread = 7
    """
    The number of pages (not including first and last) to show in the paginator widget.
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

    field_columns = {}
    """
    A dictionary of field column overrides.
    """

    field_labels = {}
    """
    A dictionary of field label overrides.
    """

    sort_fields = {}
    """
    A dictionary of sort field overrides.
    """

    highlight_fields = {}
    """
    A dictionary of highlight field overrides.
    """

    operator = getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'AND')
    """
    The query operator to use by default.
    """

    permission = None
    """
    If specified, a permission to check (using ``request.user.has_perm``) for this view.
    """

    extra_context = {}
    """
    Extra context variables to use when rendering. May be passed via as_view(), or overridden as a property.
    """

    def normalized_querystring(self, qs=None, ignore=None):
        data = QueryDict(qs) if qs is not None else self.request.GET
        parts = []
        for key in sorted(data):
            if ignore and key in ignore:
                continue
            if not data[key]:
                continue
            if key == 'p' and data[key] == '1':
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
            if isinstance(dsl_field, (dsl.Object, dsl.Nested)):
                return None
            if not isinstance(dsl_field, dsl.String):
                return field_name
            if 'raw' in dsl_field.fields:
                return '%s.raw' % field_name
            elif getattr(dsl_field, 'index', None) == 'not_analyzed':
                return field_name
        return None

    def get_field_highlight(self, field_name):
        if field_name in self.highlight_fields:
            return self.highlight_fields[field_name]
        if field_name in self.document._doc_type.mapping:
            dsl_field = self.document._doc_type.mapping[field_name]
            if isinstance(dsl_field, (dsl.Object, dsl.Nested)):
                return '%s.*' % field_name
            return field_name
        return None

    def make_column(self, field_name):
        """
        Creates a :class:`seeker.Column` instance for the given field name.
        """
        if field_name in self.field_columns:
            return self.field_columns[field_name]
        label = self.get_field_label(field_name)
        sort = self.get_field_sort(field_name)
        highlight = self.get_field_highlight(field_name)
        return Column(field_name, label=label, sort=sort, highlight=highlight)

    def get_columns(self):
        """
        Returns a list of :class:`seeker.Column` objects based on self.columns, converting any strings.
        """
        columns = []
        if not self.columns:
            # If not specified, all mapping fields will be available.
            for f in self.document._doc_type.mapping:
                if self.exclude and f in self.exclude:
                    continue
                columns.append(self.make_column(f))
        else:
            # Otherwise, go through and convert any strings to Columns.
            for c in self.columns:
                if isinstance(c, six.string_types):
                    if self.exclude and c in self.exclude:
                        continue
                    columns.append(self.make_column(c))
                elif isinstance(c, Column):
                    if self.exclude and c.field in self.exclude:
                        continue
                    columns.append(c)
        # Make sure the columns are bound and ordered based on the display fields (selected or default).
        display = self.get_display()
        for c in columns:
            c.bind(self, c.field in display)
        columns.sort(key=lambda c: display.index(c.field) if c.visible else 9999)
        return columns

    def get_keywords(self):
        return self.request.GET.get('q', '').strip()

    def get_facets(self):
        return list(self.facets) if self.facets else []

    def get_display(self):
        """
        Returns a list of display field names. If the user has selected display fields, those are used, otherwise
        the default list is returned. If no default list is specified, all fields are displayed.
        """
        default = list(self.display) if self.display else list(self.document._doc_type.mapping)
        return self.request.GET.getlist('d') or default

    def get_facet_data(self, initial=None, exclude=None):
        if initial is None:
            initial = {}
        facets = collections.OrderedDict()
        for f in self.get_facets():
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
        using = self.using or self.document._doc_type.using or 'default'
        index = self.index or self.document._doc_type.index or getattr(settings, 'SEEKER_INDEX', 'seeker')
        # TODO: self.document.search(using=using, index=index) once new version is released
        s = self.document.search().index(index).using(using).extra(track_scores=True)
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
        facets = self.get_facet_data(initial=self.initial_facets if initial else None)
        search = self.get_search(keywords, facets)
        columns = self.get_columns()

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
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.highlight for c in columns if c.visible and c.highlight]
            search = search.highlight(*highlight_fields, number_of_fragments=0)

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
            'display_columns': [c for c in columns if c.visible],
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

        if self.extra_context:
            context.update(self.extra_context)

        if initial:
            return render(self.request, self.template_name, context)
        else:
            return JsonResponse({
                'querystring': self.normalized_querystring(),
                'table_html': loader.render_to_string(self.results_template, context),
                'facet_data': {facet.field: facet.data(results) for facet in self.get_facets()},
            })

    def render_facet_query(self):
        keywords = self.get_keywords()
        facet = {f.field: f for f in self.get_facets()}.get(self.request.GET.get('_facet'))
        if not facet:
            raise Http404()
        # We want to apply all the other facet filters besides the one we're querying.
        facets = self.get_facet_data(exclude=facet)
        search = self.get_search(keywords, facets, aggregate=False)
        fq = '.*' + self.request.GET.get('_query', '').strip() + '.*'
        facet.apply(search, include={'pattern': fq, 'flags': 'CASE_INSENSITIVE'})
        return JsonResponse(facet.data(search.execute()))

    def export(self):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        keywords = self.get_keywords()
        facets = self.get_facet_data()
        search = self.get_search(keywords, facets, aggregate=False)
        columns = self.get_columns()

        def csv_escape(value):
            if isinstance(value, (list, tuple)):
                value = '; '.join(force_text(v) for v in value)
            return '"%s"' % force_text(value).replace('"', '""')

        def csv_generator():
            yield ','.join('"%s"' % c.label for c in columns if c.visible and c.export) + '\n'
            for result in search.scan():
                yield ','.join(csv_escape(c.export_value(result)) for c in columns if c.visible and c.export) + '\n'

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
        if resp is not None:
            return resp
        return super(SeekerView, self).dispatch(request, *args, **kwargs)
