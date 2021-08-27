import abc
import collections
import copy
import inspect
import itertools
import json
import re
import warnings
from datetime import datetime

import elasticsearch_dsl as dsl

from django.conf import settings
from django.contrib import messages
from django.forms.forms import Form
from django.http import Http404, JsonResponse, QueryDict, StreamingHttpResponse
from django.http.response import HttpResponseBadRequest, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.template import Context, RequestContext, TemplateDoesNotExist, loader
from django.utils import timezone
from django.utils.encoding import force_text
from django.utils.html import escape, format_html
from django.utils.http import urlencode
from django.views.generic import View
from django.views.generic.edit import CreateView, FormView
from elasticsearch_dsl import Q
from elasticsearch_dsl.utils import AttrList

from .facets import TermsFacet, RangeFilter, TextFacet
from .mapping import DEFAULT_ANALYZER
from .signals import advanced_search_performed, search_complete
from .templatetags.seeker import seeker_format
from seeker.utils import update_timestamp_index

seekerview_field_templates = {}


class Column(object):
    """
    """

    view = None
    visible = False

    def __init__(self, field, label=None, sort=None, value_format=None, template=None, header=None, export=True, highlight=None, field_definition=None):
        self.field = field
        self.label = label if label is not None else field.replace('_', ' ').replace('.raw', '').capitalize()
        self.sort = sort
        self.template = template
        self.value_format = value_format
        self.header_html = escape(self.label) if header is None else header
        self.export = export
        self.highlight = highlight
        self.field_definition = field_definition

    def __str__(self):
        return self.label

    def __repr__(self):
        return 'Column(%s)' % self.field

    def bind(self, view, visible):
        self.view = view
        self.visible = visible
        if self.visible:
            if self.template:
                self.template_obj = loader.get_template(self.template)
            else:
                self.template_obj = self.view.get_field_template(self.field)
        # Set the model_lower variable on Column to the lowercased name of the model on the mapping once view is set above
        try:
            self.model_lower = self.view.document._model
        except AttributeError:
            document = self.view.document
            if hasattr(document, 'model'):
                self.model_lower = document.model.__name__.lower()
            elif hasattr(document, 'queryset'):
                self.model_lower = document.queryset().model.__name__.lower()
            else:
                self.model_lower = ''
            self.view.document._model = self.model_lower

        return self

    def header(self):
        cls = '%s_%s' % (self.view.document._doc_type.name, self.field.replace('.', '_'))
        cls += ' %s_%s' % (self.model_lower, self.field.replace('.', '_'))
        if not self.sort:
            return format_html('<th class="{}">{}</th>', cls, self.header_html)
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
        sr_label = format_html(' <span class="sr-only">({})</span>', sort) if sort else ''
        if self.field_definition:
            span = format_html('<span title="{}" class ="fa fa-question-circle"></span>', self.field_definition)
        else:
            span = ''
        html = format_html(
            '<th class="{}"><a href="?{}" title="Click to sort {}" data-sort="{}">{}{} {}</a></th>',
            cls,
            q.urlencode(),
            next_sort,
            q['s'],
            self.header_html,
            sr_label,
            span
        )
        return html

    def context(self, result, **kwargs):
        return kwargs

    def render(self, result, **kwargs):
        value = getattr(result, self.field, None)
        try:
            if '*' in self.highlight:
                # If highlighting was requested for multiple fields, grab any matching fields as a dictionary.
                r = self.highlight.replace('*', r'\w+').replace('.', r'\.')
                highlight = {f.replace('.', '_'): result.meta.highlight[f] for f in result.meta.highlight if re.match(r, f)}
            else:
                highlight = result.meta.highlight[self.highlight]
        except Exception:
            highlight = []

        # If the value is a list (AttrList is DSL's custom list) then highlight won't work properly
        # The "meta.highlight" will only contain the matched item, not the others
        if highlight and isinstance(value, AttrList):
            # We are going to modify this copy with the appropriate highlights
            modified_values = copy.deepcopy(value)
            for highlighted_value in highlight:
                # Remove the <em> tags elasticsearch added
                stripped_value = highlighted_value.replace('<em>', '').replace('</em>', '')
                index_to_replace = None
                # Iterate over all of the values and try to find the item that caused the "hit"
                for index, individual_value in enumerate(value):
                    if stripped_value == individual_value:
                        index_to_replace = index
                        break
                # Specifically check against None because "0" is falsy (but a valid index)
                if index_to_replace is not None:
                    modified_values[index_to_replace] = highlighted_value
            highlight = modified_values

        if self.value_format:
            value = self.value_format(value)
            if highlight:
                highlight = self.value_format(highlight)

        params = {
            'result': result,
            'field': self.field,
            'value': value,
            'highlight': highlight,
            'model_lower': self.model_lower,
            'doc_class_name': result.__class__.__name__.lower(),
            'view': self.view,
            'user': self.view.request.user,
            'query': self.view.get_keywords(self.view.request.GET),
        }
        params.update(self.context(result, **kwargs))
        return self.template_obj.render(params)

    def export_value(self, result):
        export_field = self.field if self.export is True else self.export
        if export_field:
            value = getattr(result, export_field, '')
            if isinstance(value, datetime) and timezone.is_aware(value):
                value = timezone.localtime(value)
            export_val = ', '.join(force_text(v.to_dict() if hasattr(v, 'to_dict') else v) for v in value) if isinstance(value, AttrList) else seeker_format(value)
        else:
            export_val = ''
        return export_val


class SeekerView(View):
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
    The ES index to use. Will use the index set on the mapping if this is not set.
    """

    template_name = 'seeker/seeker.html'
    """
    The overall seeker template to render.
    """

    search_form_template = 'seeker/form.html'
    """
    The template to render seeker form
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

    login_required_columns = []
    """
    A list of field names that will automatically be added to the exclude when generating columns if the user is not authenticated.
    """

    display = None
    """
    A list of field/column names to display by default.
    """

    post_filter_facets = False
    """
    A boolean set to optionally define a dynamic response in the facets and results after a change to the form
    You will need to set javascript and ajax on the seeker template in order to fully enable these features
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
    A list of field/column names to sort by default, or None for no default sort order. For reverse order prefix the field with '-'.
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

    number_of_fragments = 0
    """
    The number of fragments returned by highlighted search, set to 0 by default (which gives all results)
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

    available_page_sizes = []
    """
    If set allows user to set options for page size to be changed, (must include the default page_size)
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

    field_definitions = {}
    """
    A dictionary of field definitions. These appear in the header of a column.
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
    This property is slated to be deprecated in the future. Please use "modify_context".
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

    use_save_form = False
    """
    Indicates if a form should be used for saving searches.
    NOTE: This functionality ONLY works with AJAX.
    NOTE: The form used is defined in "get_saved_search_form"
    """

    form_template = 'seeker/save_form.html'
    """
    The form template used to display the save search form.
    NOTE: This is only used if the request is AJAX and 'use_save_form' is True.
    NOTE: This template will be used to render the form defined in 'get_saved_search_form"
    TODO: This form does not exist in template and is unknown if this functionality works on SeekerView...
    TODO: Change name for clarity on next major release (save_form_template)
    """

    enforce_unique_name = True
    """
    The system will enforce the unique name requirement.
    All previously existing saved searches (in the same group) with the same name as the new one will be deleted.
    """

    display_column_sort_order = []
    """
    This list defines a custom sort order for the display fields (both visible & non-visible).  If the list is empty, default sorting will be applied.
    If there is at least one field in the list, any missing field will be appended to the end of the list in alphabetical order by column label.
    NOTE: The indexes defined in required_display are not used if display_column_sort_order has a value.
    """

    custom_column_headers = {}
    """
    This dictionary can be used to set custom text for a fields column header.  The key is the field_name.
    """

    analyzer = DEFAULT_ANALYZER
    """
    The ES analyzer used for keyword searching.
    """

    missing_sort = None
    """
    Whether to sort missing values first or last. Valid values are "_first", "_last", "_low", "_high", or None.
    """

    search_extra = {}
    """
    A dictionary of "extra" properties to add to the DSL search object.
    For example: search_extra = {'track_total_hits': True}
    """

    search_params = {}
    """
    A dictionary of "parameters" to add to the DSL search object.
    For example: search_params = {'routing': '42}
    """

    def modify_context(self, context, request):
        """
        This function allows modifications to the context that will be used to render the initial seeker page.
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass

    def get_page_size(self):
        ps = self.request.GET.get('page_size', '').strip()
        try:
            return int(ps) if int(ps) > 0 and int(ps) in self.available_page_sizes else self.page_size
        except ValueError:
            return self.page_size

    def modify_results_context(self, context):
        """
        This function allows modifications to the context that will be used to render the results table.
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass

    view_name = None
    """
    An optional name to call this view, used to differentiate two views using the same mapping and class.
    """

    def get_saved_search_form(self):
        """
        Get the form used to save searches.
        NOTE: This will only be used if 'use_save_form' is set to True and with AJAX requests.
        NOTE: This form will be passed the "saved_searches" kwarg when instantiated.
        """
        from .forms import SavedSearchForm
        return SavedSearchForm

    def get_view_name(self):
        """
        Returns the view_name if set, otherwise return the class name and document name.
        """
        if self.view_name:
            return self.view_name
        else:
            return self.__class__.__name__ + self.document._doc_type.name

    def normalized_querystring(self, qs=None, ignore=None):
        """
        Returns a querystring with empty keys removed and keys in sorted order.
        Suitable for saving and comparing searches.

        :param qs: (Optional) querystring to use; defaults to request.GET
        :param ignore: (Optional) list of keys to ignore when building the querystring
        """
        data = QueryDict(qs) if qs is not None else self.request.GET
        parts = []
        for key in sorted(data):
            if ignore and key in ignore:
                continue
            values = data.getlist(key)
            if not any(values):
                continue
            if key == 'p' and data[key] == '1':
                continue
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
            f = self.document.model._meta.get_field(field_name)
            return f.verbose_name[0].upper() + f.verbose_name[1:]
        except Exception:
            try:
                f = self.document.queryset().model._meta.get_field(field_name)
                return f.verbose_name[0].upper() + f.verbose_name[1:]
            except Exception:
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
            if not isinstance(dsl_field, dsl.Text):
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
        if not self._field_templates:
            try:
                self._field_templates = seekerview_field_templates[self.get_view_name()]
            except KeyError:
                seekerview_field_templates.update({self.get_view_name(): {}})
                self._field_templates = seekerview_field_templates[self.get_view_name()]
        try:
            return self._field_templates[field_name]
        except KeyError:
            return self._find_field_template(field_name)

    def _find_field_template(self, field_name):
        """
        finds and sets the default template instance for the given field name with the given template.
        """
        search_templates = []
        if field_name in self.field_templates:
            search_templates.append(self.field_templates[field_name])
        if hasattr(self.document, 'model'):
            search_templates.append('seeker/{}/{}.html'.format(self.document.model.__name__.lower(), field_name))
        elif hasattr(self.document, 'queryset'):
            search_templates.append('seeker/{}/{}.html'.format(self.document.queryset().model.__name__.lower(), field_name))
        for _cls in inspect.getmro(self.document):
            if issubclass(_cls, dsl.Document):
                search_templates.append('seeker/{}/{}.html'.format(_cls.__name__.lower(), field_name))
        search_templates.append('seeker/column.html')
        template = loader.select_template(search_templates)
        existing_templates = list(set(self._field_templates.values()))
        for existing_template in existing_templates:
            # If the template object already exists just re-use the existing one.
            if template.template.name == existing_template.template.name:
                template = existing_template
                break
        self._field_templates.update({field_name: template})
        return template

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
        header = self.custom_column_headers.get(field_name, None)
        field_definition = self.field_definitions.get(field_name)
        return Column(field_name, label=label, sort=sort, highlight=highlight, header=header, field_definition=field_definition)

    def sort_columns(self, columns, display=None):
        if not display:
            display = self.get_display()

        if self.display_column_sort_order:
            sort_order = self.get_sort_order(columns)
            columns.sort(key=lambda c: sort_order.index(c.field))
            return columns
        else:
            visible_columns = []
            non_visible_columns = []
            for c in columns:
                if c.visible:
                    visible_columns.append(c)
                else:
                    non_visible_columns.append(c)
            visible_columns.sort(key=lambda  c: display.index(c.field))
            non_visible_columns.sort(key=lambda c: c.label)

            return visible_columns + non_visible_columns

    def get_columns(self, display=None, sort=True):
        """
        Returns a list of :class:`seeker.Column` objects based on self.columns, converting any strings.
        """
        columns = []
        exclude = copy.copy(self.exclude) or []
        if not self.request.user.is_authenticated:
            exclude += self.login_required_columns

        if not self.columns:
            # If not specified, all mapping fields will be available.
            for f in self.document._doc_type.mapping:
                if exclude and f in exclude:
                    continue
                columns.append(self.make_column(f))
        else:
            # Otherwise, go through and convert any strings to Columns.
            for c in self.columns:
                if isinstance(c, str):
                    if exclude and c in exclude:
                        continue
                    columns.append(self.make_column(c))
                elif isinstance(c, Column):
                    if exclude and c.field in exclude:
                        continue
                    columns.append(c)
        # Make sure the columns are bound and ordered based on the display fields (selected or default).
        if not display:
            display = self.get_display()

        for c in columns:
            c.bind(self, c.field in display)

        if sort:
            columns = self.sort_columns(columns, display)

        return columns

    def get_sorted_display_list(self):
        return self.request.GET.getlist("so")

    def get_sort_order(self, columns):
        sort_order = self.get_sorted_display_list() or self.display_column_sort_order
        # Missing columns is a list of every column that isn't included in sort_order.
        # These columns will be appended to the end of the display list in alphabetical order.
        missing_columns = [col for col in columns if col.field not in sort_order]
        missing_columns.sort(key=lambda c: c.label)
        sort_order += [col.field for col in missing_columns]
        for field, i in self.required_display:
            sort_order.insert(i, field)
        return sort_order

    def get_keywords(self, data_dict):
        return data_dict.get('q', '').strip()

    def get_facets(self):
        facets = []
        for facet in self.facets:
            if self.request.user.is_authenticated or not facet.login_required:
                facets.append(facet) 
        return facets

    def get_sorts(self):
        return self.request.GET.getlist('s', None)

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

    def get_facet_data(self, data_dict, initial=None, exclude=None):
        if initial is None:
            initial = {}
        facets = collections.OrderedDict()
        for f in self.get_facets():
            if f.field != exclude:
                facets[f] = data_dict.getlist(f.field) or initial.get(f.field, [])
        return facets

    def get_saved_search_model(self):
        from .models import SavedSearch
        return SavedSearch

    def get_saved_searches(self):
        if self.request.user and self.request.user.is_authenticated:
            return self.request.user.seeker_searches.filter(url=self.request.path)
        else:
            return []

    def get_search_fields(self, mapping=None, prefix=''):
        if self.search:
            return self.search
        elif mapping is not None:
            fields = []
            for field_name in mapping:
                if mapping[field_name].to_dict().get('analyzer') == self.analyzer:
                    fields.append(prefix + field_name)
                if hasattr(mapping[field_name], '_mapping'):
                    fields.extend(self.get_search_fields(mapping=mapping[field_name]._mapping, prefix=prefix + field_name + '.'))
            return fields
        else:
            return self.get_search_fields(mapping=self.document._doc_type.mapping)

    def get_search_query_type(self, search, keywords, analyzer=None):
        if not analyzer:
            analyzer = self.analyzer
        kwargs = {'query': keywords,
                 'analyzer': analyzer,
                 'fields': self.get_search_fields(),
                 'default_operator': self.operator}
        if self.query_type == 'simple_query':
            kwargs['auto_generate_phrase_queries'] = True
        return search.query(self.query_type, **kwargs)

    def get_search(self, keywords=None, facets=None, aggregate=True):
        using = self.using or self.document._index._using or 'default'
        index = self.index or self.document._index
        # TODO: self.document.search(using=using, index=index) once new version is released
        s = (
            self.document.search()
            .index(index)
            .using(using)
            .extra(track_scores=True, **self.search_extra)
            .params(**self.search_params)
        )
        if keywords:
            s = self.get_search_query_type(s, keywords)
        if facets:
            for facet, values in facets.items():
                if values:
                    s = facet.filter(s, values)
                if aggregate:
                    facet.apply(s)
        return s

    def apply_sort_descriptor(self, sort):
        desc = sort.startswith('-')
        field = sort.lstrip('-')
        missing = self.missing_sort
        if missing == '_low':
            missing = '_last' if desc else '_first'
        elif missing == '_high':
            missing = '_first' if desc else '_last'
        return {
                field: {
                    'order': 'desc' if desc else 'asc',
                    'missing': missing,
                }
            }

    def sort_descriptor(self, sort):
        if self.missing_sort is None or isinstance(sort, dict):
            return sort
        if not isinstance(sort, list):
            return self.apply_sort_descriptor(sort)
        else:
            sort_dict = {}
            for s in sort:
                sort_dict.update(self.apply_sort_descriptor(s))
            return sort_dict

    def is_initial(self):
        """
        Returns true if the request is in an initial search, false if not
        Can be overridden in case of non initial get requests that aren't ajax (submitted form)
        This will depend on the seeker form html and the info coming in on the get request
        """
        return not self.request.is_ajax()

    def apply_highlight(self, search, columns):
        highlight_fields = self.highlight if isinstance(self.highlight, (list, tuple)) else [c.highlight for c in columns if c.highlight]
        # NOTE: If the option to customize the tags (via pre_tags and post_tags) is added then the Column "render" function will need to be updated.
        search = search.highlight(*highlight_fields, number_of_fragments=self.number_of_fragments).highlight_options(encoder=self.highlight_encoder)
        return search

    def render(self):
        SavedSearchModel = self.get_saved_search_model()

        querystring = self.normalized_querystring(ignore=['p', 'saved_search'])

        if self.request.user and self.request.user.is_authenticated and not querystring and not self.request.is_ajax():
            default = self.request.user.seeker_searches.filter(url=self.request.path, default=True).first()
            if default and default.querystring:
                return redirect(default)

        # Figure out if this is a saved search, and grab the current user's saved searches.
        saved_search = None
        saved_searches = self.get_saved_searches()
        saved_search_pk = self.get_saved_search()
        if saved_searches and saved_search_pk:
            try:
                saved_search = saved_searches.get(pk=saved_search_pk)
            except SavedSearchModel.DoesNotExist:
                pass

        keywords = self.get_keywords(self.request.GET)
        facets = self.get_facet_data(self.request.GET, initial=self.initial_facets if self.is_initial()  else None)
        search = self.get_search(keywords, facets)
        columns = self.get_columns()

        if self.post_filter_facets:
            executed_search = search.execute()
            facets_selected_and_results = collections.OrderedDict()
            for facet in facets:
                if facets[facet]:
                    stored_facet_data = facets[facet]
                    facets[facet] = []
                    facets_selected_and_results[facet] = (stored_facet_data, self.get_search(keywords, facets).execute())
                    facets[facet] = stored_facet_data
                else:
                    facets_selected_and_results[facet] = (facets[facet], executed_search)
        else:
            facets_selected_and_results = None
        # Make sure we sanitize the sort fields.
        sort_fields = []
        column_lookup = {c.field: c for c in columns}
        sorts = self.get_sorts()
        if not sorts:
            if keywords:
                sorts = []
            else:
                sorts = self.sort or []
        for s in sorts:
            # Get the column based on the field name, and use it's "sort" field, if applicable.
            c = column_lookup.get(s.lstrip('-'))
            if c and c.sort:
                sort_fields.append(self.sort_descriptor('-%s' % c.sort if s.startswith('-') else c.sort))

        # Highlight fields.
        if self.highlight:
            search = self.apply_highlight(search, columns)

        # Calculate paging information.
        page_size = self.get_page_size()
        page = self.request.GET.get('p', '').strip()
        page = int(page) if page.isdigit() else 1
        offset = (page - 1) * page_size
        if search.count() <= offset:
            page = 1
            offset = 0

        # Finally, grab the results.
        results = search.sort(*sort_fields)[offset:offset + page_size].execute()
        context_querystring = self.normalized_querystring(ignore=['p'])
        sort = sorts[0] if sorts else None
        context = {
            'document': self.document,
            'keywords': keywords,
            'columns': columns,
            'optional_columns': [c for c in columns if c.field not in self.required_display_fields],
            'display_columns': [c for c in columns if c.visible],
            'facets': facets,
            'post_filter_facets': self.post_filter_facets,
            'facets_selected_and_results': facets_selected_and_results,
            'selected_facets': self.request.GET.getlist('f') or self.initial_facets,
            'form_action': self.request.path,
            'results': results,
            'total_hits': search.count(),
            'page': page,
            'page_size': page_size,
            'available_page_sizes': self.available_page_sizes,
            'page_spread': self.page_spread,
            'sort': sort,
            'querystring': context_querystring,
            'reset_querystring': self.normalized_querystring(ignore=['p', 's', 'saved_search']),
            'show_rank': self.show_rank,
            'export_name': self.export_name,
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated,
            'header_template': self.header_template,
            'search_form_template': self.search_form_template,
            'results_template': self.results_template,
            'footer_template': self.footer_template,
            'saved_search': saved_search,
            'saved_searches': list(saved_searches),
            'use_save_form': self.use_save_form,
        }
        if self.use_save_form:
            SavedSearchForm = self.get_saved_search_form()
            form = SavedSearchForm(saved_searches=saved_searches)
            context.update({
                'save_form': form,
                'save_form_template': self.form_template
            })

        if self.extra_context:
            context.update(self.extra_context)

        self.modify_context(context, self.request)

        search_complete.send(sender=self, context=context)
        if self.request.is_ajax():
            ajax_data = {
                'querystring': context_querystring,
                'page': page,
                'sort': sort,
                'saved_search_pk': saved_search.pk if saved_search else '',
                'table_html': loader.render_to_string(self.results_template, context, request=self.request),
                'facet_data': {facet.field: facet.data(results) for facet in self.get_facets()},
            }
            if self.use_save_form:
                ajax_data.update({
                    'save_form_html': loader.render_to_string(self.form_template, { 'form': form }, request=self.request)
                })
            if self.post_filter_facets:
                ajax_data.update({
                    'form_html': loader.render_to_string(self.search_form_template, context, request=self.request)
                })
            return JsonResponse(ajax_data)
        else:
            return self.render_to_response(context)

    def render_to_response(self, context):
        return render(self.request, self.template_name, context)

    def render_facet_query(self):
        keywords = self.get_keywords(self.request.GET)
        facet = {f.field: f for f in self.get_facets()}.get(self.request.GET.get('_facet'))
        if not facet:
            raise Http404()
        # We want to apply all the other facet filters besides the one we're querying.
        facets = self.get_facet_data(self.request.GET, exclude=facet)
        search = self.get_search(keywords, facets, aggregate=False)
        fq = '.*' + self.request.GET.get('_query', '').strip() + '.*'
        facet.apply(search, include={'pattern': fq, 'flags': 'CASE_INSENSITIVE'})
        return JsonResponse(facet.data(search.execute()))

    def modify_initial_facets(self):
        facet_fields = [facet.field for facet in self.get_facets()]
        missing_facets = set(self.initial_facets.keys()).difference(set(facet_fields))
        [self.initial_facets.pop(missing_facet, None) for missing_facet in missing_facets]

    def export(self):
        """
        A helper method called when ``_export`` is present in ``request.GET``. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """
        keywords = self.get_keywords(self.request.GET)
        facets = self.get_facet_data(self.request.GET)
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
            # A "sub" method that handles ajax save submissions (and returns JSON, not a redirect)
            if request.is_ajax() and self.use_save_form:

                response_data = {}  # All data must be able to flatten to JSON

                # First we check if the user is attempting to overwrite an existing search
                saved_searches = request.user.seeker_searches.filter(url=request.path)
                form_kwargs = { 'saved_searches': saved_searches, 'enforce_unique_name': self.enforce_unique_name }
                saved_search_pk = request.POST.get('saved_search', '').strip()
                if saved_search_pk:
                    # We really want to do a try/catch on a get but we don't know the model so we check first
                    saved_search = saved_searches.exists(pk=saved_search_pk)
                    if saved_search:
                        # Since we are using "pk" we know there can only be one so .get is safe
                        form_kwargs['instance'] = saved_searches.get(pk=saved_search_pk)

                SavedSearchForm = self.get_saved_search_form()
                form = SavedSearchForm(request.POST.copy(), **form_kwargs)
                if form.is_valid():
                    saved_search = form.save(commit=False)
                    saved_search.user = request.user
                    saved_search.querystring = qs
                    saved_search.url = request.path
                    saved_search.save()
                    form.save_m2m()
                    messages.success(request, 'Successfully saved "%s".' % saved_search)
                    response_data['redirect_url'] = saved_search.get_absolute_url()
                else:
                    response_data['redirect_url'] = None

                response_data['save_form_html'] = loader.render_to_string(self.form_template, { 'form': form }, request=request)

                # We came in via ajax so we return via JSON
                return JsonResponse(response_data)

            # If the request is not ajax or this seeker view does not want to use the form, then handle it the old way
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
            request.user.seeker_searches.filter(pk=saved_search_pk, url=request.path).update(default=True)
        elif '_unset' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(url=request.path).update(default=False)
        elif '_delete' in request.POST and saved_search_pk:
            request.user.seeker_searches.filter(pk=saved_search_pk, url=request.path).delete()
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
        if resp is None:
            self.modify_initial_facets()
            resp = super().dispatch(request, *args, **kwargs)

        index = self.index or self.document._index
        update_timestamp_index(index)

        return resp


class AdvancedColumn(Column):

    def header(self, results=None):
        cls = '{}_{}'.format(self.view.document._doc_type.name, self.field.replace('.', '_'))
        cls += ' {}_{}'.format(self.view.document.__name__.lower(), self.field.replace('.', '_'))
        if self.model_lower:
            cls += ' {}_{}'.format(self.model_lower, self.field.replace('.', '_'))
        if not self.sort:
            return format_html('<th class="{}">{}</th>', cls, self.header_html)
        current_sort = self.view.search_object['sort']
        sort = None
        cls += ' sort'
        if current_sort.lstrip('-') == self.field:
            # If the current sort field is this field, give it a class a change direction.
            sort = 'Descending' if current_sort.startswith('-') else 'Ascending'
            cls += ' desc' if current_sort.startswith('-') else ' asc'
            d = '' if current_sort.startswith('-') else '-'
            data_sort = '{}{}'.format(d, self.field)
        else:
            data_sort = self.field

        next_sort = 'descending' if sort == 'Ascending' else 'ascending'
        sr_label = format_html(' <span class="sr-only">({})</span>', sort) if sort else ''

        # If results provided, we check to see if header has space to allow for wordwrapping. If it already wordwrapped
        # (i.e. has <br> in header) we skip it.
        if results and ' ' in self.header_html and not '<br' in self.header_html:
            if len(self.header_html) > self.get_data_max_length(results):
                self.wordwrap_header_html()
        if self.field_definition:
            span = format_html('<span title="{}" class ="fa fa-question-circle"></span>', self.field_definition)
        else:
            span = ''
        html = format_html(
            '<th class="{}"><a href="#" title="Click to sort {}" data-sort="{}">{}{} {}</a></th>',
            cls, next_sort, data_sort, self.header_html, sr_label, span
        )
        return html

    def get_data_max_length(self, results):
        """
        Determines maximum length of data populating the column of field_name
        :param results: search results from elastic search
        :return: maximum length of data, or 0 if the field_name does not exist or the is no data
        """
        max_length = 0
        for result in results.hits:
            field_len = len(str(result[self.field])) if (self.field in result and result[self.field]) else 0
            max_length = max(max_length, field_len)
        return max_length

    def wordwrap_header_html(self):
        """
        Trying to get 2 parts with lengths as close as possible
        :param field_name: field_name (header_html) that needs to be broken into 2 pieces.
        :return: field_name broken into 2 pieces, separated by <br /> tag
        """
        if self.header_html.count(' ') < 1:
            return
        center = len(self.header_html) // 2
        space_found = False
        offset = 0
        space_index = center
        while not space_found:
            for index in [center + offset, center - offset]:
                if self.header_html[index] == ' ':
                    space_found = True
                    space_index = index
            offset += 1
        self.header_html = format_html("{}<br/>{}", self.header_html[:space_index], self.header_html[space_index + 1:])

    def export_value(self, result):
        export_field = self.field if self.export is True else self.export
        if export_field:
            value = getattr(result, export_field, '')
            if isinstance(value, datetime) and timezone.is_aware(value):
                value = timezone.localtime(value)
            elif isinstance(value, AttrList):
                value = ', '.join(force_text(v.to_dict() if hasattr(v, 'to_dict') else v) for v in value)
            if self.value_format:
                value = self.value_format(value)
            export_val = seeker_format(value)
        else:
            export_val = ''
        return export_val


class AdvancedSeekerView(SeekerView):
    available_page_sizes = [10, 20, 50, 100]
    """
    A list of available page sizes. The values must be integers.
    NOTE: These values are NOT enforced, they are simply used for rendering.
    """

    boolean_translations = {
        'AND': 'must',
        'OR': 'should'
    }
    """
    This dictionary translates the boolean operators passed from the frontend into their elasticsearch equivalents.
    """

    footer_template = 'advanced_seeker/footer.html'
    """
    The template used to render the search results footer.
    """

    header_template = 'advanced_seeker/header.html'
    """
    The template used to render the search results header.
    """

    results_template = 'advanced_seeker/results.html'
    """
    The template used to render the search results.
    """

    template_name = 'advanced_seeker/seeker.html'
    """
    The overall seeker template to render.
    """

    use_wordwrap_header = False
    """
    If set to True, table headers will wrap to second line if they are longer than the content and have space available
    between words.
    """

    search_timeout = 10
    """
    The number of seconds to allow any DSL search to execute before a timeout error is raised.
    """

    always_display_highlighted_columns = False
    """
    If True, any column that includes a cell with highlighted text will be automaticaly displayed by the system.
    """

    @abc.abstractproperty
    def save_search_url(self):
        pass

    """
    This property should return the url of the associated AdvancedSavedSearchView.
    It is set to abstract because it needs to be defined on a site by site basis. None is a valid value if saved searches are not being used.
    """

    @abc.abstractproperty
    def search_url(self):
        pass

    """
    This property should return the url of this seeker view.
    It is set to abstract because it needs to be defined on a site by site basis.
    Generally, this will be a 'reverse' call to the URL associated with this view.
    """

    sort = ''
    """
    Default field to sort by. Prepend '-' to reverse sort.
    """

    add_facets_to_display = True
    """
    Facet fields with selected values will automatically be added to the 'display' list (shown on the results table).
    """

    value_formats = {}
    """
    key: field_name, value: value_format function
    A dictionary of custom formats used for extracting values from mappings. 
    value_formats are used for creating columns and exporting values in csv files.
    """

    separate_aggregation_search = False
    """
    If True, aggregations will be executed in a separate DSL Search object.  This will allows sites to cache aggregation searches.
    """

    default_column_class = AdvancedColumn
    """
    This is the default class that make_column will use when create seeker columns.
    """

    field_column_classes = {}
    """
    A dictionary of field_name -> AdvancedColumn class that allows you to easily customize a column class and still utilize the make_column method.
    """

    def __init__(self):
        if vars(SeekerView).get('get_search_query_type') != getattr(self, 'get_search_query_type').__func__:
            warnings.warn(
                "'get_search_query_type' function is deprecated, please use 'get_keyword_query' instead.",
                DeprecationWarning
            )

    def modify_json_response(self, json_response, context):
        """
        This function allows modifications to the json data that will be returned when rendering results.
        The context used to render the results template is passed in as a convenience.
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass

    def modify_aggregation_json_response(self, json_response, context):
        """
        This function allows modifications to the json data that will be returned when rendering facet aggregations.
        NOTE: The changes to context should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass

    def get_display(self, display_list, facets_searched):
        """
        Returns a list of display field names. If the user has selected display fields and display_list is not empty those are used otherwise
        the default list is returned. If no default list is specified, all fields are displayed.
        """
        facet_lookup = { facet.field: facet for facet in self.get_facets() }
        default = list(self.display) if self.display else list(self.document._doc_type.mapping)
        display_list = display_list or default

        if self.add_facets_to_display:
            for field in facets_searched:
                related_column_name = facet_lookup[field].related_column_name
                if related_column_name not in display_list + self.required_display:
                    display_list.append(related_column_name)

        display_fields = [f for f in display_list if f not in self.required_display_fields]
        for field, i in self.required_display:
            display_fields.insert(i, field)
        return display_fields

    def get_search(self, keywords=None, facets=None, aggregate=True):
        s = self.get_dsl_search()
        if keywords:
            # TODO - Once 'get_search_query_type' is removed this can be cleaned up to:
            # s.query(self.get_keyword_query(keywords))
            s = self.get_search_query_type(s, keywords)
        if facets:
            for facet, values in facets.items():
                if values:
                    s = facet.filter(s, values)
                if aggregate:
                    facet.apply(s)
        return s

    def get_dsl_search(self):
        using = self.using or self.document._index._using or 'default'
        index = self.index or self.document._index
        # TODO: self.document.search(using=using, index=index) once new version is released
        return (
            self.document.search()
            .index(index)
            .using(using)
            .extra(track_scores=True, **self.search_extra)
            .params(**self.search_params)
        )

    def make_column(self, field_name):
        """
        Creates a :class:`seeker.Column` instance for the given field name.
        """
        if field_name in self.field_columns:
            return self.field_columns[field_name]
        label = self.get_field_label(field_name)
        sort = self.get_field_sort(field_name)
        highlight = self.get_field_highlight(field_name)
        header = self.custom_column_headers.get(field_name, None)
        val_format = self.value_formats.get(field_name, None)
        field_definition = self.field_definitions.get(field_name)

        column_class = self.field_column_classes.get(field_name, self.default_column_class)
        return column_class(
            field_name,
            label=label,
            sort=sort,
            highlight=highlight,
            header=header,
            value_format=val_format,
            field_definition=field_definition
        )

    def apply_sort_field(self, column_lookup, sort):
        c = column_lookup.get(sort.lstrip('-'))
        if c and c.sort:
            return('-{}'.format(c.sort) if sort.startswith('-') else c.sort)
        return sort

    def get_sort_field(self, columns, sort, display):
        """
        Returns the appropriate sort field for a given sort value.
        """

        # Make sure we sanitize the sort fields.
        sort_fields = []
        column_lookup = { c.field: c for c in columns }
        # Order of precedence for sort is: parameter, the default from the view, and then the first displayed column (if any are displayed)
        sort = sort or self.sort or display[0] if len(display) else ''
        # Get the column based on the field name, and use it's "sort" field, if applicable.
        if not isinstance(sort, list):
            return self.apply_sort_field(column_lookup, sort)
        else:
            for s in sort:
                sort_fields.append(self.apply_sort_field(column_lookup, s))
        return sort_fields

    def get_sorted_display_list(self):
        return self.search_object.get("displaySortOrder")

    def get(self, request, *args, **kwargs):
        facets = self.get_facets()
        context = {
            'can_save': self.can_save and self.request.user and self.request.user.is_authenticated,
            'facets': facets,
            'search_url': self.search_url,
            'save_search_url': self.save_search_url,
            'selected_facets': self.initial_facets,
            'initial_search_object_query': self.initial_facet_query()
        }

        if self.extra_context:
            context.update(self.extra_context)

        self.modify_context(context, request)
        return self.render_to_response(context)

    def post(self, request, *args, **kwargs):
        """
        Parameters:
            'search_object' - A json string representing a dictionary in the following format:
                {
                    'query': (dictionary) The boolean query to be performed (see 'advanced_search' for format),
                    'keywords': (string) Used to search the specified fields (see 'search' attr). NOTE: This is SUBTRACTIVE ONLY!
                    'page': (integer) The page of the search,
                    'sort': (string) The field name to be used when sorting (prepend '-' for reverse),
                    'display': (list) The string field names to be displayed in the result table. NOTE: This is ordered!
                }
            '_export' - If present, the return value will be a CSV file of the results.
        NOTE: The 'search_object' parameter key values listed here are the only fields that are required.
              Since it will be passed back in the response extra values can be added to give the site context as to what search is being loaded.
        """
        export = request.POST.get('_export', False)
        if request.is_ajax() or export:
            try:
                string_search_object = request.POST.get('search_object')
                # We attach this to self so AdvancedColumn can have access to it
                self.search_object = json.loads(string_search_object)
            except KeyError:
                return HttpResponseBadRequest("No 'search_object' found in the request data.")
            except ValueError:
                return HttpResponseBadRequest("Improperly formatted 'search_object', json.loads failed.")

            # Sanity check that the search object has all of it's required components
            if not all(k in self.search_object for k in ('query', 'keywords', 'page', 'sort', 'display')):
                return HttpResponseBadRequest("The 'search_object' is not in the proper format.")

            # If the search_object has an active_facet, seeker will aggregate and return aggregation results
            if self.search_object.get('active_facet'):
                return self.get_facet_aggregations()
            else:
                return self.render_results(export)
        else:
            return HttpResponseBadRequest("This endpoint only accepts AJAX requests.")

    def _get_processing_data(self):
        """A helper function that generates common data shared between get_facet_aggregations and render_results"""
        facet_lookup = { facet.field: facet for facet in self.get_facets() }

        # This "query" is the dictionary of rules, conditions, etc. (see build_query)
        query = self.search_object.get('query')

        # Build the actual query that will be applied via post_filter
        advanced_query, facets_searched = self.build_query(query, facet_lookup)

        # For issues with speed this function should be used to limit the number of facets as much as possible
        facet_lookup = self.filter_facet_lookup(facet_lookup, facets_searched)

        return facet_lookup, query, advanced_query, facets_searched

    def apply_keywords(self, search):
        """Applies keywords to the search if they exist in the search_object"""
        keywords = self.search_object['keywords'].strip()
        if keywords:
            search = self.get_search_query_type(search, keywords)
        return search

    def display_highlighted_columns(self, columns, display, results):
        highlight_results = [list(hit.meta.highlight.to_dict().keys()) for hit in results if hasattr(hit.meta, 'highlight')]
        highlighted_columns = list(set(itertools.chain.from_iterable(highlight_results)))
        self.search_object['system_added_display'] = []
        for column in columns:
            if not column.visible and column.field in highlighted_columns:
                column.bind(self, True)
                self.search_object['system_added_display'].append(column.field)

        columns = self.sort_columns(columns, display + self.search_object['system_added_display'])

        return columns

    def get_aggregated_results(self, query, facet_lookup):
        """This function gets the aggregation results for a query.  It does not get the search results"""
        # We build a whole new search because apply_aggregations somehow breaks the search object being "immutable"
        aggregation_search = self.get_dsl_search()
        aggregation_search = self.apply_keywords(aggregation_search)
        aggregation_search = self.additional_query_filters(aggregation_search)
        self.apply_aggregations(aggregation_search, query, facet_lookup)
        # In order to utilize cache we need to set the size to 0 and turn off scoring
        aggregation_results = aggregation_search[0:0].extra(track_scores=False).params(request_timeout=self.search_timeout).execute()

        return aggregation_search, aggregation_results

    def get_facet_aggregations(self):
        facet_lookup, query, advanced_query, facets_searched = self._get_processing_data()
        aggregation_search, aggregation_results = self.get_aggregated_results(query, facet_lookup)
        context = {
            'facet_lookup': facet_lookup,
            'query': query,
            'advanced_query': advanced_query,
            'facets_searched': facets_searched,
            'aggregation_search': aggregation_search,
            'aggregation_results': aggregation_results
        }

        json_response = {
            'filters': [facet.build_filter_dict(aggregation_results) for facet in facet_lookup.values()],  # Relies on the default 'apply_aggregations' being applied.
            'search_object': self.search_object
        }

        self.modify_aggregation_json_response(json_response, context)

        return JsonResponse(json_response)

    def initial_facet_query(self):

        initial_query = {'condition': self.initial_facets.get('condition', 'AND'), 'rules': []}
        for facet in self.get_facets():
            if facet.field in self.initial_facets:
                if hasattr(facet, 'initialize') and self.initial_facets[facet.field]:
                    initial_query['rules'].append(facet.initialize(self.initial_facets[facet.field]))
        return json.dumps(initial_query)

    def render_results(self, export):
        facet_lookup, query, advanced_query, facets_searched = self._get_processing_data()

        search = self.get_dsl_search()

        # Hook to allow the search to be filtered before seeker begins it's work
        search = self.additional_query_filters(search)

        # The default aggregation is across all available documents (minus additional_query_filters)
        # If a subclass would like to aggregate in a different way (post filter for example) they have access
        # to self.search_object, which they can use to do so.
        aggregation_results = None
        if self.separate_aggregation_search:
            aggregation_search, aggregation_results = self.get_aggregated_results(query, facet_lookup)

        # If there are any keywords passed in, we combine the advanced query with the keyword query
        search = self.apply_keywords(search)

        if self.separate_aggregation_search:
            # We apply the actual query (keywords have been applied already, if applicable)
            search = search.query(advanced_query)
        else:
            # We use post_filter to allow the aggregations to be run before applying the filter
            search = search.post_filter(advanced_query)

        page_size = int(self.search_object.get('page_size', self.page_size))
        page, offset = self.calculate_page_and_offset(self.search_object['page'], page_size, search)

        display = self.get_display(self.search_object['display'], facets_searched)
        columns = self.get_columns(display, sort=not self.always_display_highlighted_columns)
        if export:
            return self.export(search, columns)

        if not self.separate_aggregation_search:
            self.apply_aggregations(search, query, facet_lookup)

        # Highlight fields.
        if self.highlight:
            search = self.apply_highlight(search, columns)

        # Finally, grab the results.
        sort = self.get_sort_field(columns, self.search_object['sort'], display)
        if sort:
            if (self.missing_sort is None or isinstance(sort, dict)) and isinstance(sort, list):
                results = search.sort(*self.sort_descriptor(sort))[offset:offset + page_size].params(request_timeout=self.search_timeout).execute()
            else:
                results = search.sort(self.sort_descriptor(sort))[offset:offset + page_size].params(request_timeout=self.search_timeout).execute()
        else:
            results = search[offset:offset + page_size].params(request_timeout=self.search_timeout).execute()

        if not self.separate_aggregation_search:
            aggregation_results = results

        if self.always_display_highlighted_columns:
            columns = self.display_highlighted_columns(columns, display, results)

        # TODO clean this up (may not need everything)
        context = {
            'columns': columns,
            'display_columns': [c for c in columns if c.visible],
            'system_added_display': self.search_object.get('system_added_display', []),
            'facet_lookup': facet_lookup,
            'facets_searched': facets_searched,
            'footer_template': self.footer_template,
            'header_template': self.header_template,
            'optional_columns': [c for c in columns if c.field not in self.required_display_fields],
            'page': page,
            'page_spread': self.page_spread,
            'available_page_sizes': self.available_page_sizes,
            'page_size': page_size,
            'query': query,
            'aggregation_results': aggregation_results,
            'results': results,
            'total_hits': search.count(),
            'show_rank': self.show_rank,
            'sort': sort,
            'export_name': self.export_name,
            'use_wordwrap_header': self.use_wordwrap_header,
            'search': search
        }
        self.modify_results_context(context)

        json_response = {
            'filters': [facet.build_filter_dict(aggregation_results) for facet in facet_lookup.values()],  # Relies on the default 'apply_aggregations' being applied.
            'table_html': loader.render_to_string(self.results_template, context, request=self.request),
            'search_object': self.search_object,
        }

        self.modify_json_response(json_response, context)
        advanced_search_performed.send(sender=self.__class__, request=self.request, context=context, json_response=json_response)
        return JsonResponse(json_response)

    def calculate_page_and_offset(self, page, page_size, search):
        offset = (page - 1) * page_size
        if search.count() <= offset:
            page = 1
            offset = 0
        return page, offset

    def apply_aggregations(self, search, query, facet_lookup):
        """
        Applies the desired aggregations to the search.
        By default this function applies each facet individually.
        NOTE: This function makes the modification of the search object in place, there is no return value.
        NOTE: It is recommended that any function overwriting this should call super (or replicate the aggregations done here).
              If that doesn't happen then the 'filters' dictionary may not be build appropriately.
        """
        if self.search_object.get('active_facet'):
            facet = facet_lookup[facet_name]
            facet.apply(search)
        else:
            for facet in facet_lookup.values():
                facet.apply(search)

    def additional_query_filters(self, search):
        """
        Allows additional search filters (Q objects) to be applied to the search.
        Ideally, filters applied by this function will be applied for every search.
        For that reason nothing is passed to this function except the search.
        """
        return search

    def filter_facet_lookup(self, facet_lookup, facets_searched, **kwargs):
        """
        Allows the list of facets to be reduced as much as possible. The decision on what can be
        reduced is up to the individual site so the default returns facet_lookup unaltered.
        NOTE: The more facets that can be removed from this list the better the response time will be for the search.
        """
        return facet_lookup

    def build_query(self, advanced_query, facet_lookup, excluded_facets=[]):
        """
        Returns two values:
        1) The ES DSL Q object representing the 'advanced_query' dictionary passed in
        2) A list of the selected fields for this query

        The advanced_query is a dictionary representation of the advanced query. The following is an example of the accepted format:
        {
            "condition": "<boolean operator>",
            "rules": [
                {
                    "id": "<elasticsearch field id>",
                    "operator": "<comparison operator>",
                    "value": "<search value>"
                },
                {
                    "condition": "<boolean operator>",
                    "rules": [
                        {
                            "id": "<elasticsearch field id>",
                            "operator": "<comparison operator>",
                            "value": "<search value>"
                        }, ...
                    ],
                    "not": <flag to negate sibling rules>
                }, ...
            ],
            "not": <flag to negate sibling rules>
        }

        NOTES:
        Each 'rule' is a dictionary containing single rules and groups of rules. The value for each rule field are as follows:
            - id:     The name of the field in the elasticsearch document being searched.
            - operator:  A key in COMPARISON_CONVERSION dictionary. It is up to you to ensure the operator will work with the given field.
            - value:     The value to be used in the comparison for this rule
        Each group of rules will have:
            - condition: The boolean operator to apply to all rules in this group.
            - rules: A list of dictionaries containing either groups or rules.
            - not: A boolean (true/false) to indicate if this group should be additive or subtractive to the search.
        """
        # Check if all required keys are present for an individual rule
        if all(k in advanced_query for k in ('id', 'operator', 'value')):
            if advanced_query['id'] not in excluded_facets:
                facet = facet_lookup.get(advanced_query['id'])
                return facet.es_query(advanced_query['operator'], advanced_query['value']), [facet.field]
            return None, None

        # Check if all required keys are present for a group
        elif all(k in advanced_query for k in ('condition', 'rules')):
            condition = advanced_query.get('condition')
            group_operator = self.boolean_translations.get(condition, None)
            if not group_operator:
                raise ValueError("'{}' is not a valid boolean operator.".format(condition))

            queries = []
            selected_facets = []
            # The central portion of the recursion, we iterate over all rules inside this group
            for dict in advanced_query.get('rules'):
                query, facet_field = self.build_query(dict, facet_lookup, excluded_facets)
                if query:
                    queries.append(query)
                    selected_facets += facet_field

            if advanced_query.get('not', False):
                return ~Q('bool', **{group_operator: queries}), list(set(selected_facets))
            else:
                return Q('bool', **{group_operator: queries}), list(set(selected_facets))

        # The advanced_query must have been missing something, so we cannot create this query
        else:
            raise ValueError("The dictionary passed in did not have the proper structure. Dictionary: {}".format(str(advanced_query)))

    def export(self, search, columns):
        """
        A helper method called when ``_export`` is present in the http request. Returns a ``StreamingHttpResponse``
        that yields CSV data for all matching results.
        """

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


class SearchFailed(Exception):
    """ Thrown when a search fails """
    pass


class AdvancedSavedSearchView(View):
    pk_parameter = 'saved_search_pk'
    """
    The parameter to check for to get the saved search id (either via URL or request GET/POST)
    """

    url_parameter = 'url'
    """
    The parameter to check for to get the url associated with the desired saved searches.
    """

    restrict_to_user = True
    """
    If users should only be able to view their own saved searches.
    """

    form_template = 'advanced_seeker/save_form.html'
    """
    The form template used to display the save search form.
    """

    success_return_empty_form = True
    """
    When a search is successfully saved this determines if an empty form should be returned (instead of one linked the the instance just saved)
    """

    enforce_unique_name = True
    """
    The system will enforce the unique name requirement.
    All previously existing saved searches (in the same group) with the same name as the new one will be deleted.
    """

    def get(self, request, *args, **kwargs):
        if self.request.is_ajax():
            try:
                url = request.GET.get(self.url_parameter)
            except KeyError:
                return JsonResponse({ 'error': 'No URL provided.' }, 400)

            SavedSearchModel = self.get_saved_search_model()
            saved_searches = self.get_saved_searches(url, SavedSearchModel)
            search_pk = kwargs.get(self.pk_parameter, request.GET.get(self.pk_parameter, None))
            if search_pk:
                try:
                    saved_search = saved_searches.get(pk=search_pk)
                except SavedSearchModel.DoesNotExist:
                    return JsonResponse({ 'error': 'Saved search not found.' }, 400)
            else:
                # By design this will return None if there are no default searches found
                saved_search = saved_searches.filter(default=True).first()

            # If a saved search is found we include its data in 'current_search' for convenience (it will also be in 'all_searches')
            data = { 'current_search': saved_search.get_details_dict() if saved_search else None }

            SavedSearchForm = self.get_saved_search_form()

            # Even if a specific saved search was not found we return all of the other available saved searches
            if saved_searches:
                data.update({ 'all_searches': self.sort_searches([saved_search.get_details_dict() for saved_search in saved_searches]) })

            # Hook to allow customization
            self.update_GET_response_data(data, saved_search)

            form = SavedSearchForm(saved_searches=saved_searches)
            data['form_html'] = loader.render_to_string(self.form_template, { 'form': form }, request=self.request)

            return JsonResponse(data)
        else:
            return HttpResponseBadRequest("This endpoint only accepts AJAX requests.")

    def post(self, request, *args, **kwargs):
        if self.request.is_ajax():
            try:
                url = request.POST.get(self.url_parameter)
            except KeyError:
                return JsonResponse({'error': 'No URL provided.'}, 400)

            search_pk = kwargs.get(self.pk_parameter, request.POST.get(self.pk_parameter, None))
            SavedSearchModel = self.get_saved_search_model()
            saved_searches = self.get_saved_searches(url, SavedSearchModel)

            # These are used to determine alternate paths
            delete = request.POST.get('_delete', False)
            modify_default = request.POST.get('modify_default', '')

            saved_search = None
            if search_pk:
                try:
                    saved_search = saved_searches.get(pk=search_pk)
                except SavedSearchModel.DoesNotExist:
                    # We only want to throw an error if we cannot find the object AND we are NOT trying to delete it anyway
                    if not delete:
                        return JsonResponse({'error': 'Saved search not found.'}, 400)

            data = {}
            # We have three paths: delete, modify_default, or save
            status = 200
            SavedSearchForm = self.get_saved_search_form()
            form_kwargs = { 'saved_searches': saved_searches, 'enforce_unique_name': self.enforce_unique_name }
            if delete:
                if saved_search:
                    saved_search.delete()
                saved_search = None  # 'saved_search' will still hold the python object of the saved search that was just deleted
            elif modify_default:
                if modify_default == 'set':
                    saved_search.default = True
                    saved_searches.update(default=False)
                    saved_search.save()
                elif modify_default == 'unset':
                    saved_search.default = False
                    saved_search.save()
                else:
                    data["error"] = 'Invalid value for "modify_default" field'
                    saved_search = None
                    status = 400
            else:
                if saved_search:
                    form_kwargs['instance'] = saved_search
                # We use a copy of POST to allow it to be edited within the form (example, if a field should be cleared on error)
                form = SavedSearchForm(request.POST.copy(), **form_kwargs)
                if form.is_valid():
                    saved_search = form.save(commit=False)
                    saved_search.user = request.user
                    saved_search.save()
                    form.save_m2m()

                    # TODO - Look for way to prevent us from needing to rerun this query to update results
                    saved_searches = self.get_saved_searches(url, SavedSearchModel)
                    if self.success_return_empty_form:
                        # We need a fresh copy to pass back to the site
                        form = SavedSearchForm(saved_searches=saved_searches)
                else:
                    # This error message is likely redundant
                    data["error"] = 'Invalid form submitted'
                    saved_search = None
                    status = 400

            # the saved search form needs to be updated if we modified a default search or deleted a search
            if delete or modify_default:
                form = SavedSearchForm(**form_kwargs)

            # We add the form here because we want to return the most up-to-date version of the form
            data['form_html'] = loader.render_to_string(self.form_template, { 'form': form }, request=self.request)

            # 'current_search' is included in 'all_searches' but seperated for convenience
            data['current_search'] = saved_search.get_details_dict() if saved_search else None

            # We do this query again to make sure we don't have stale data.
            data['all_searches'] = self.sort_searches([search.get_details_dict() for search in saved_searches])

            # Hook to allow customization
            self.update_POST_response_data(data, saved_search)

            return JsonResponse(data, status=status)
        else:
            return HttpResponseBadRequest("This endpoint only accepts AJAX requests.")

    def update_GET_response_data(self, data, saved_search=None):
        """
        This function allows modifications to the json data that will be returned with a GET request.
        The 'saved_search' being loaded may be passed in for convenience (it may be None).
        NOTE: The changes to data should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass

    def update_POST_response_data(self, data, saved_search=None):
        """
        This function allows modifications to the json data that will be returned with a POST request.
        The 'saved_search' being loaded may be passed in for convenience (it may be None).
        NOTE: The changes to data should be done in place. This function does not have a return (similar to 'dict.update()').
        """
        pass

    def sort_searches(self, all_searches):
        """
        This function sorts the list of searches that will be returned.
        """
        return sorted(all_searches, key=lambda search: search.get('name'))

    def get_saved_search_model(self):
        from .models import SavedSearch
        return SavedSearch

    def get_saved_search_form(self):
        """
        Get the form used to save searches.
        NOTE: This form will be passed the "saved_searches" kwarg when instantiated.
        """
        from .forms import AdvancedSavedSearchForm
        return AdvancedSavedSearchForm

    def get_saved_searches(self, url, SavedSearchModel):
        """
        Returns a QuerySet of the saved searches for this particular request (based on URL and seeker settings)
        """
        filter_kwargs = { 'url': url }
        if self.restrict_to_user:
            if self.request.user.is_authenticated:
                filter_kwargs['user'] = self.request.user
            else:
                return SavedSearchModel.objects.none()
        return SavedSearchModel.objects.filter(**filter_kwargs)
