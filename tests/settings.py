from elasticsearch_dsl.connections import connections

import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Make sure the copy of seeker in the directory above this one is used.
sys.path.insert(0, BASE_DIR)

SECRET_KEY = 'seeker_tests__this_is_not_very_secret'

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'seeker',
    'core',
)

ROOT_URLCONF = 'urls'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

SEEKER_INDEX = 'seeker-tests'

connections.configure(default={'hosts': 'localhost'})
