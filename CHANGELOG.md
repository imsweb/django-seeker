# 6.1.1

* **BREAKING CHANGE:** the old form_template has been redefined to save_form_template, if you have previously redefined that template on a seeker view change it to save_form_template, now form_template defines the form inside the seeker template not the save_form

* Added is_dynamic option seeker view to allow users to easily setup dynamic ajax responses on changes to facets in the form. Users will need to setup their own javascript to enable these features.
