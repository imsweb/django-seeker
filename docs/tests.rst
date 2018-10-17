Running Tests
=============

The Seeker unit tests are based on `Django's testing framework`_.

To run the tests, first you'll need to update tests/settings.py with
the URL(s) of your elasticsearch server(s) and the username and
password that you use to authenticate on them.

After configuring your settings file, create a virtualenv_ for seeker to run in and install its dependencies with:

    cd tests
    pip install -r requirements.txt

Finally, you should be able to run the tests with:

    python manage.py test

.. _`Django's testing framework`: https://docs.djangoproject.com/en/1.7/topics/testing/
.. _virtualenv: http://virtualenv.org/
