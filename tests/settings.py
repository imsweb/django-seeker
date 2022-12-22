import os
import sys

from seeker.dsl import connections

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Make sure the copy of seeker in the directory above this one is used.
sys.path.insert(0, BASE_DIR)

SECRET_KEY = 'seeker_tests__this_is_not_very_secret'

MIDDLEWARE = (
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
