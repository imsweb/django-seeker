from django.conf import settings
from django.contrib import messages
from django.http import Http404, JsonResponse, QueryDict, StreamingHttpResponse
from django.shortcuts import redirect, render
from django.template import Context, RequestContext, loader, TemplateDoesNotExist
from django.utils import timezone
from django.utils.encoding import force_text
from django.utils.html import escape
from django.utils.http import urlencode
from django.utils.safestring import mark_safe
from django.views.generic import View
from elasticsearch_dsl.utils import AttrList
import elasticsearch_dsl as dsl
import six

from seeker.templatetags.seeker import seeker_format

from .mapping import DEFAULT_ANALYZER

import collections
import inspect
import re


class Column (object):
    """
    """

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
        if self.visible:
            self.template_obj = self.view.get_field_template(self.field)
            if self.template:
                try:
                    self.template_obj = loader.get_template(self.template)
                except TemplateDoesNotExist:
                    pass
        return self

    def header(self):
        cls = '%s_%s' % (self.view.document._doc_type.name, self.field.replace('.', '_'))
        if not self.sort:
            return mark_safe('<th class="%s">%s</th>' % (cls, self.header_html))
        q = self.view.request.GET.copy()
        field = q.get('s', '')
        sort = None
        cls += ' sort'
        if field.lstrip('-') == self.field:
            # If the current sort field is this field, give it a class a change direction.
            sort = 'Descending' if field.startswith('-') else 'Ascending'
            cls += ' desc' if field.startswith('-') else ' asc'
            d = '' if field.startswith('-') else '-'
            q['s'] = '%s%s' % (d, self.field)
        else:
            q['s'] = self.field
        next_sort = 'descending' if sort == 'Ascending' else 'ascending'
        sr_label = (' <span class="sr-only">(%s)</span>' % sort) if sort else ''
        html = '<th class="%s"><a href="?%s" title="Click to sort %s" data-sort="%s">%s%s</a></th>' % (cls, q.urlencode(), next_sort, q['s'], self.header_html, sr_label)
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
        return self.template_obj.render(params)

    def export_value(self, result):
        export_field = self.field if self.export is True else self.export
        if export_field:
            value = getattr(result, export_field, '')
            export_val = ', '.join(force_text(v.to_dict() if hasattr(v, 'to_dict') else v) for v in value) if isinstance(value, AttrList) else seeker_format(value)
        else:
            export_val = ''
        return export_val


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

    header_template = 'seeker/header.html'
    """
    The template used to render the search results header.
    """

    results_template = 'seeker/results.html'
    """
    The template used to render the search results.
    """

    footer_template = 'seeker/footer.html'
    """
    The template used to render the search results footer.
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

    required_display = []
    """
    A list of tuples, ex. ('field name', 0), representing field/column names that will always be displayed (cannot be hidden by the user).
    The second value is the index/position of the field (used as the index in list.insert(index, 'field name')).
    """

    @property
    def required_display_fields(self):
        return [t[0] for t in self.required_display]

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

    highlight_encoder = 'html'
    """
    An 'encoder' parameter is used when highlighting to define how highlighted text will be encoded. It can be either
    'default' (no encoding) or 'html' (will escape html, if you use html highlighting tags).
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

    export_timestamp = False
    """
    Whether or not to append a timestamp of the current time to the export filename when exporting data from this view.
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

    query_type = getattr(settings, 'SEEKER_QUERY_TYPE', 'query_string')
    """
    The query type to use when performing keyword queries (either 'query_string' (default) or 'simple_query_string').
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
    
    field_templates = {}
    """
    A dictionary of field template overrides.
    """
    
    _field_templates = {}
    """
    A dictionary of default templates for each field
    """

    def normalized_querystring(self, qs=None, ignore=None):
        """
        Returns a querystring with empty keys removed, keys in sorted order, and values (for keys whose order does not
        matter) in sorted order. Suitable for saving and comparing searches.

        :param qs: (Optional) querystring to use; defaults to request.GET
        :param ignore: (Optional) list of keys to ignore when building the querystring
        """
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
            parts.extend(urlencode({key: val}) for val in values)
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
            return f.verbose_name[0].upper() + f.verbose_name[1:]
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

    def get_field_template(self, field_name):
        """
        Returns the default template instance for the given field name.
        """
        try:
            return self._field_templates[field_name]
        except KeyError:
            return self.find_field_template(field_name)

    @classmethod
    def find_field_template(cls, field_name):
        """
        finds and sets the default template instance for the given field name with the given template.
        """
        search_templates = []
        if field_name in cls.field_templates:
            search_templates.append(cls.field_templates[field_name])
        for _cls in inspect.getmro(cls.document):
            if issubclass(_cls, dsl.DocType):
                search_templates.append('seeker/%s/%s.html' % (_cls._doc_type.name, field_name))
        search_templates.append('seeker/column.html')
        template = loader.select_template(search_templates)
        existing_templates = list(set(cls._field_templates.values()))
        for existing_template in existing_templates:
            #If the template object already exists just re-use the existing one.
            if template.template.name == existing_template.template.name:
                template = existing_template
                break
        cls._field_templates.update({field_name: template})
        return template

    @classmethod
    def update_field_template(cls, field_name, template):
        """
        Updates the _field_template instance of field_name with template object for the entire class
        """
        cls._field_templates.update({field_name: template})

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
        visible_columns = []
        non_visible_columns=[]
        for c in columns:
            c.bind(self, c.field in display)
            if c.visible:
                visible_columns.append(c)
            else:
                non_visible_columns.append(c)
        visible_columns.sort(key=lambda  c: display.index(c.field))
        non_visible_columns.sort(key=lambda c: c.label)
        visible_columns.extend(non_visible_columns)
        columns=visible_columns[:]
        
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
        display_fields = self.request.GET.getlist('d') or default
        display_fields = [f for f in display_fields if f not in self.required_display_fields]
        for field, i in self.required_display:
            display_fields.insert(i, field)
        return display_fields

    def get_saved_search(self):
        """
        Returns the "saved_search" GET parameter if it's in the proper format, otherwise returns None.
        """
        saved_search_vals = [val for val in self.request.GET.getlist('saved_search') if val]
        if len(saved_search_vals) == 1 and saved_search_vals[0].isdigit():
            return saved_search_vals[0]
        return None

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
                if mapping[field_name].to_dict().get('analyzer') == DEFAULT_ANALYZER:
                    fields.append(prefix + field_name)
                if hasattr(mapping[field_name], 'properties'):
                    fields.extend(self.get_search_fields(mapping=mapping[field_name].properties, prefix=prefix + field_name + '.'))
            return fields
        else:
            return self.get_search_fields(mapping=self.document._doc_type.mapping)

    def get_search_query_type(self, search, keywords, analyzer=DEFAULT_ANALYZER):
        kwargs = {'query': keywords,
                  'analyzer': analyzer,
                  'fields': self.get_search_fields(),
                  'default_operator': self.operator}
        if self.query_type == 'simple_query':
            kwargs['auto_generate_phrase_queries'] = True
        return search.query(self.query_type, **kwargs)

    def get_search(self, keywords=None, facets=None, aggregate=True):
        using = self.using or self.document._doc_type.using or 'default'
        index = self.index or self.document._doc_type.index or getattr(settings, 'SEEKER_INDEX', 'seeker')
        # TODO: self.document.search(using=using, index=index) once new version is released
        s = self.document.search().index(index).using(using).extra(track_scores=True)
        if keywords:
            s = self.get_search_query_type(s, keywords)
        if facets:
            for facet, values in facets.items():
                if values:
                    s = facet.filter(s, values)
                if aggregate:
                    facet.apply(s)
        return s

    def render(self):
        from .models import SavedSearch

        querystring = self.normalized_querystring(ignore=['p', 'saved_search'])

        if self.request.user and self.request.user.is_authenticated() and not querystring and not self.request.is_ajax():
            default = self.request.user.seeker_searches.filter(url=self.request.path, default=True).first()
            if default and default.querystring:
                return redirect(default)

        # Figure out if this is a saved search, and grab the current user's saved searches.
        saved_search = None
        if self.request.user and self.request.user.is_authenticated():
            saved_search_pk = self.get_saved_search()
            if saved_search_pk:
                try:
                    saved_search = self.request.user.seeker_searches.get(pk=saved_search_pk, url=self.request.path, querystring=querystring)
                except SavedSearch.DoesNotExist:
                    pass
            saved_searches = list(self.request.user.seeker_searches.filter(url=self.request.path))
        else:
            saved_searches = []

        keywords = self.get_keywords()
        facets = self.get_facet_data(initial=self.initial_facets if not self.request.is_ajax() else None)
        search = self.get_search(keywords, facets)
        columns = self.get_columns()

        # Make sure we sanitize the sort fields.
        sort_fields = []
        column_lookup = {c.field: c for c in columns}
        sorts = self.request.GET.getlist('s', None)
        if not sorts:
            if keywords:
                sorts = []
            else:
                sorts = self.sort or []
        for s in sorts:
            # Get the column based on the field name, and use it's "sort" field, if applicable.
            c = column_lookup.get(s.lstrip('-'))
            if c and c.sort:
                sort_fields.append('-%s' % c.sort if s.startswith('-') else c.sort)

        # Highlight fields.
        if self.highlight:
            highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.highlight for c in columns if c.visible and c.highlight]
            search = search.highlight(*highlight_fields, number_of_fragments=0).highlight_options(encoder=self.highlight_encoder)

        # Calculate paging information.
        page = self.request.GET.get('p', '').strip()
        page = int(page) if page.isdigit() else 1
        offset = (page - 1) * self.page_size
        results_count = search[0:0].execute().hits.total
        if results_count < offset:
            page = 1
            offset = 0

        # Finally, grab the results.
        results = search.sort(*sort_fields)[offset:offset + self.page_size].execute()

        context_querystring = self.normalized_querystring()
        sort = sorts[0] if sorts else None
        context = {
            'document': self.document,
            'keywords': keywords,
            'columns': columns,
            'optional_columns': [c for c in columns if c.field not in self.required_display_fields],
            'display_columns': [c for c in columns if c.visible],
            'facets': facets,
            'selected_facets': self.request.GET.getlist('f') or self.initial_facets.keys(),
            'form_action': self.request.path,
            'results': results,
            'page': page,
            'page_size': self.page_size,
            'page_spread': self.page_spread,
            'sort': sort,
            'querystring': context_querystring,
            'reset_querystring': self.normalized_querystring(ignore=['p', 's', 'saved_search']),
            'show_rank': self.show_rank,
            'export_name': self.export_name,
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated(),
            'header_template': self.header_template,
            'results_template': self.results_template,
            'footer_template': self.footer_template,
            'saved_search': saved_search,
            'saved_searches': saved_searches,
        }

        if self.extra_context:
            context.update(self.extra_context)

        if self.request.is_ajax():
            return JsonResponse({
                'querystring': context_querystring,
                'page': page,
                'sort': sort,
                'saved_search_pk': saved_search.pk if saved_search else '',
                'table_html': loader.render_to_string(self.results_template, context, request=self.request),
                'facet_data': {facet.field: facet.data(results) for facet in self.get_facets()},
            })
        else:
            return render(self.request, self.template_name, context)

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

        export_timestamp = ('_' + timezone.now().strftime('%m-%d-%Y_%H-%M-%S')) if self.export_timestamp else ''
        export_name = '%s%s.csv' % (self.export_name, export_timestamp)
        resp = StreamingHttpResponse(csv_generator(), content_type='text/csv; charset=utf-8')
        resp['Content-Disposition'] = 'attachment; filename=%s' % export_name
        return resp

    def get(self, request, *args, **kwargs):
        if '_facet' in request.GET:
            return self.render_facet_query()
        elif '_export' in request.GET:
            return self.export()
        else:
            return self.render()

    def post(self, request, *args, **kwargs):
        if not self.can_save:
            return redirect(request.get_full_path())
        post_qs = request.POST.get('querystring', '')
        qs = self.normalized_querystring(post_qs, ignore=['p', 'saved_search'])
        saved_search_pk = request.POST.get('saved_search', '').strip()
        if not saved_search_pk.isdigit():
            saved_search_pk = None
        if '_save' in request.POST:
            name = request.POST.get('name', '').strip()
            if not name:
                messages.error(request, 'You did not provide a name for this search. Please try again.')
                return redirect('%s?%s' % (request.path, post_qs))
            default = request.POST.get('default', '').strip() == '1'
            if default:
                request.user.seeker_searches.filter(url=request.path).update(default=False)
            search_values = {'querystring': qs, 'default': default}
            search, created = request.user.seeker_searches.update_or_create(name=name, url=request.path, defaults=search_values)
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
        return redirect('%s?%s' % (request.path, post_qs))

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
