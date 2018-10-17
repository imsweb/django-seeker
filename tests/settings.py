import sys
import os

# CHANGE THESE SETTINGS to work with your configuration:

SEEKER_HOSTS = [
    'http://my-elasticsearch-server-01.com:9200',
    'http://my-elasticsearch-server-02.com:9200'
]
SEEKER_HTTP_AUTH = 'USERNAME:PASSWORD'


# The settings below shouldn't need to be customized

SEEKER_INDEX = 'unique-seeker-index-928374'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Make sure the copy of seeker in the directory above this one is used.
sys.path.insert(0, BASE_DIR)

SECRET_KEY = 'seeker_tests__this_is_not_very_secret'

INSTALLED_APPS = (
    'seeker',
    'core',
    'django.contrib.auth',
    'django.contrib.contenttypes',
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
