from django import template
from django.utils.html import escape
from django.http import QueryDict
from django.contrib.humanize.templatetags.humanize import intcomma
import string

register = template.Library()

@register.simple_tag
def facet_checkbox(facet, value, filters=None, missing='MISSING', count_prefix=''):
    if filters is None:
        filters = {}
    key = facet.get_key(value)
    return '<label><input type="checkbox" name="%(name)s" value="%(key)s"%(checked)s data-count="%(count)s" /> %(key_fmt)s (%(count_prefix)s%(count_fmt)s)</label>' % {
        'name': facet.field,
        'key': key or '',
        'key_fmt': key or missing,
        'count': value['doc_count'],
        'count_fmt': intcomma(value['doc_count']),
        'count_prefix': count_prefix,
        'checked': ' checked="checked"' if facet.field in filters and key in filters[facet.field] else '',
    }

@register.simple_tag
def facet_values(facet, filters, missing='MISSING', remove='&times;'):
    html = '<ul class="list-unstyled facet-values">'
    for term in filters.get(facet.field, []):
        if not term:
            term = missing
        html += '<li><a class="remove" data-term="%(term)s" title="Remove this term">%(remove)s</a> %(term)s</li>' % {'term': escape(term), 'remove': remove}
    html += '</ul>'
    return html

@register.filter
def list_display(values, sep=', '):
    return sep.join(unicode(v) for v in values)

@register.simple_tag
def sort_link(sort_by, label=None, querystring='', name='sort', mapping=None):
    q = QueryDict(querystring).copy()
    parts = q.get(name, '').split(':')
    if parts[0] and parts[0] == sort_by:
        if len(parts) > 1:
            d = '' if parts[1] == 'desc' else 'desc'
            cur = parts[1] or 'asc'
        else:
            d = 'desc'
            cur = 'asc'
    else:
        d = cur = ''
    q[name] = '%s:%s' % (sort_by, d) if d else sort_by
    if label is None:
        if mapping:
            label = mapping.field_label(sort_by)
        else:
            label = string.capwords(sort_by.replace('_', ' '))
    return '<a href="?%s" class="sort %s">%s</a>' % (q.urlencode(), cur, escape(label))

@register.simple_tag
def field_label(mapping, field_name):
    return mapping.field_label(field_name)

@register.simple_tag
def result_value(result, field_name):
    value = result.data.get(field_name, '')
    if value is None:
        return ''
    if isinstance(value, (list, tuple)):
        return list_display(value)
    return value

@register.simple_tag
def suggest_link(suggestions, querystring='', name='q'):
    q = QueryDict(querystring).copy()
    keywords = q.get(name, '').strip()
    for term, replacement in suggestions.iteritems():
        keywords = keywords.replace(term, replacement)
    q[name] = keywords
    return '<a href="?%s" class="suggest">%s</a>' % (q.urlencode(), escape(keywords))
