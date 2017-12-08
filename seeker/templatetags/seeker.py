from django import template
from django.contrib.humanize.templatetags.humanize import intcomma
from django.core.paginator import Paginator
from django.template import loader
from django.utils.encoding import force_text
from django.utils.safestring import mark_safe
from six.moves.urllib.parse import parse_qsl
import six

import datetime
import re
import urllib


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
    if isinstance(keep, basestring):
        keep = [keep]
    qs_parts = [part for part in parse_qsl(qs, keep_blank_values=True) if part[0] in keep]
    return urllib.urlencode(qs_parts)


@register.simple_tag
def seeker_facet(facet, results, selected=None, **params):
    params.update({
        'facet': facet,
        'selected': selected,
        'data': facet.data(results),
    })
    return loader.render_to_string(facet.template, params)


@register.simple_tag
def seeker_column(column, result, **kwargs):
    return column.render(result, **kwargs)


@register.simple_tag
def seeker_score(result, max_score=None, template='seeker/score.html'):
    pct = result.meta.score / max_score if max_score else 0.0
    return loader.render_to_string(template, {
        'score': result.meta.score,
        'max_score': max_score,
        'percentile': pct * 100.0,
    })


@register.simple_tag
def seeker_pager(total, page_size=10, page=1, param='p', querystring='', spread=7, template='seeker/pager.html', max_results=None):
    print total
    if max_results:
        paginator = Paginator(range(min(total,(max_results//page_size * page_size))), page_size)
    else:
        paginator = Paginator(range(total), page_size)
    if paginator.num_pages < 2:
        return ''
    page = paginator.page(page)
    if paginator.num_pages > spread:
        start = max(1, min(paginator.num_pages + 1 - spread, page.number - (spread // 2)))
        end = min(start + spread, paginator.num_pages + 1)
        page_range = range(start, end)
    else:
        page_range = paginator.page_range
    if max_results and max_results < page_size * total:
        show_last_page = False
    else:
        show_last_page = True
    return loader.render_to_string(template, {
        'page_range': page_range,
        'paginator': paginator,
        'page': page,
        'param': param,
        'querystring': querystring,
        'show_last_page' : show_last_page
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
    except:
        stemWord = lambda word: word
        stemWords = lambda words: words
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
