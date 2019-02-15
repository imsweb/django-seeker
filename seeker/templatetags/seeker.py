from __future__ import division

import datetime
import re

import six
from django import template
from django.contrib.humanize.templatetags.humanize import intcomma
from django.core.paginator import Paginator
from django.template import loader
from django.utils.encoding import force_text
from django.utils.safestring import mark_safe
from six.moves.urllib.parse import parse_qsl, urlencode


register = template.Library()

# Convenience so people don't need to install django.contrib.humanize
register.filter(intcomma)


@register.filter
def seeker_format(value):
    if value is None:
        return ''
    # TODO: settings for default list separator and date formats?
    if isinstance(value, datetime.datetime):
        return value.strftime('%m/%d/%Y %H:%M:%S')
    if isinstance(value, datetime.date):
        return value.strftime('%m/%d/%Y')
    if hasattr(value, '__iter__') and not isinstance(value, six.string_types):
        return ', '.join(force_text(v) for v in value)
    return force_text(value)


@register.filter
def seeker_filter_querystring(qs, keep):
    if isinstance(keep, six.string_types):
        keep = [keep]
    qs_parts = [part for part in parse_qsl(qs, keep_blank_values=True) if part[0] in keep]
    return urlencode(qs_parts)


@register.simple_tag
def seeker_facet(facet, results, selected=None, **params):
    params.update({
        'facet': facet,
        'selected': selected,
        'data': facet.data(results),
    })
    return loader.render_to_string(facet.template, params)

@register.simple_tag
def advanced_seeker_facet(facet, **params):
    """
    Renders an empty facet (no options/selections involved).
    The options/selected values should be set via AJAX.
    """
    params.update({
        'facet': facet,
    })
    return loader.render_to_string(facet.advanced_template, params)

@register.simple_tag
def seeker_column(column, result, **kwargs):
    return column.render(result, **kwargs)

@register.simple_tag
def seeker_column_header(column, results=None):
    return column.header(results)

@register.simple_tag
def seeker_score(result, max_score=None, template='seeker/score.html'):
    pct = result.meta.score / max_score if max_score else 0.0
    return loader.render_to_string(template, {
        'score': result.meta.score,
        'max_score': max_score,
        'percentile': pct * 100.0,
    })


@register.simple_tag
def seeker_pager(total, page_size=10, page=1, param='p', querystring='', spread=7, template='seeker/pager.html'):
    paginator = Paginator(list(range(total)), page_size)
    if paginator.num_pages < 2:
        return ''
    page = paginator.page(page)
    if paginator.num_pages > spread:
        start = max(1, min(paginator.num_pages + 1 - spread, page.number - (spread // 2)))
        end = min(start + spread, paginator.num_pages + 1)
        page_range = range(start, end)
    else:
        page_range = paginator.page_range
    return loader.render_to_string(template, {
        'page_range': page_range,
        'paginator': paginator,
        'page': page,
        'param': param,
        'querystring': querystring,
    })


_phrase_re = re.compile(r'"([^"]*)"')


@register.simple_tag
def seeker_highlight(text, query, algorithm='english'):
    if not query:
        return mark_safe(seeker_format(text))
    try:
        import snowballstemmer
        stemmer = snowballstemmer.stemmer(algorithm)
        stemWord = stemmer.stemWord
        stemWords = stemmer.stemWords
    except Exception:
        def stemWord(word): return word
        def stemWords(words): return words
    phrases = _phrase_re.findall(query)
    keywords = [w.lower() for w in re.split(r'\W+', _phrase_re.sub('', query)) if w]
    highlight = set(stemWords(keywords))
    text = seeker_format(text)
    for phrase in phrases:
        text = re.sub('(' + re.escape(phrase) + ')', r'<em>\1</em>', text, flags=re.I)
    parts = []
    for word in re.split(r'(\W+)', text):
        if stemWord(word.lower()) in highlight:
            parts.append('<em>%s</em>' % word)
        else:
            parts.append(word)
    return mark_safe(''.join(parts))
