Configuration
=============

Seeker Settings
---------------

SEEKER_INDEX
~~~~~~~~~~~~

Default: ``seeker``

The name of the OS index that should be used by default. This can be overridden per mapping.


SEEKER_INDEX_PREFIX
~~~~~~~~~~~~~~~~~~~

Default: ``seeker``

The PREFIX used for each OS index created.


SEEKER_INDEX_SETTINGS
~~~~~~~~~~~~~~~~~~~~~

Default: ``{}``

Default settings to be used for Indexes

SEEKER_DEFAULT_OPERATOR
~~~~~~~~~~~~~~~~~~~~~~~

Default: ``AND``

The default operator to use when performing keyword queries. This can be overridden per view.


SEEKER_BATCH_SIZE
~~~~~~~~~~~~~~~~~

Default: ``1000``

The default indexing batch size.


SEEKER_DEFAULT_FACET_TEMPLATE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``seeker/facets/terms.html``

The default template to use when rendering facets. Can be overridden per facet.


SEEKER_MAPPING_MODULE
~~~~~~~~~~~~~~~~~~~~~

Default: ``mappings``

The name of the python module to try to automatically import from each app. Setting to ``False`` or ``None`` will cause
seeker to skip doing any automatic imports.


SEEKER_DEFAULT_ANALYZER
~~~~~~~~~~~~~~~~~~~~~~~

Default: ``snowball``

The analyzer to use by default when creating ``seeker.String`` fields. Also used by default in ``SeekerView``
to determine how query strings should be analyzed (it's important that queries are analyzed the same way as your data).


SEEKER_DOCUMENT_FIELD_OVERRIDE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}``

Allows the user to set django models to the desired domain specific language, instead of using provided defaults. This overrides to update
 the document_field method defaults, and to add keys, for example can be used to add TextField = seeker.Text() to the default
instead of using RawString, with setting: SEEKER_DOCUMENT_FIELD_OVERRIDE = {models.TextField: seeker.Text()}
