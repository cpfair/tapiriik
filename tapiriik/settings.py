import os
from datetime import datetime
# Django settings for tapiriik project.

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@example.com'),
)

MANAGERS = ADMINS

ALLOWED_HOSTS = ["tapiriik.com", ".tapiriik.com", "localhost"]

USE_X_FORWARDED_HOST = True

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/var/www/example.com/media/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = 'C:/wamp/www/tapiriik/tapiriik/static/'

# URL prefix for static files.
# Example: "http://example.com/static/", "http://static.example.com/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'pipeline.finders.PipelineFinder',
#    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

STATICFILES_STORAGE = 'pipeline.storage.PipelineCachedStorage'

PIPELINE_JS = {
    'tapiriik-js': {
        'source_filenames': (
          'js/jquery.address-1.5.min.js',
          'js/tapiriik.js',
        ),
        'output_filename': 'js/tapiriik.min.js',
    },
    'tapiriik-user-js': {
        'source_filenames': (
          'js/jstz.min.js',
          'js/tapiriik-ng.js',
        ),
        'output_filename': 'js/tapiriik-user.min.js',
    }
}

PIPELINE_CSS = {
    'tapiriik-css': {
        'source_filenames': (
          'css/style.css',
        ),
        'output_filename': 'css/style.min.css',
    },
}

PIPELINE_DISABLE_WRAPPER = True

# Make this unique, and don't share it with anybody.
# and yes, this is overriden in local_settings.py
SECRET_KEY = 'vag26gs^t+_y0msoemqo%_5gb*th(i!v$l6##bq9tu2ggcsn13'

# In production, webservers must have only the public key
CREDENTIAL_STORAGE_PUBLIC_KEY = b"NotTheRealKeyFYI"
CREDENTIAL_STORAGE_PRIVATE_KEY = None

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'tapiriik.web.startup.Startup',
    'tapiriik.web.startup.ServiceWebStartup',
    'tapiriik.auth.SessionAuth'
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

SESSION_ENGINE = "django.contrib.sessions.backends.signed_cookies"  # file-based sessions on windows are terrible

ROOT_URLCONF = 'tapiriik.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'tapiriik.wsgi.application'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    "I:/wamp/www/tapiriik/tapiriik/web/templates",
)

TEMPLATE_CONTEXT_PROCESSORS = (
    'tapiriik.web.views.ab_experiment_context',
    'tapiriik.web.context_processors.user',
    'tapiriik.web.context_processors.config',
    'tapiriik.web.context_processors.js_bridge',
    'tapiriik.web.context_processors.stats',
    'tapiriik.web.context_processors.providers',
    'tapiriik.web.context_processors.celebration_mode',
    'django.core.context_processors.static',)

INSTALLED_APPS = (
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'tapiriik.web',
    'pipeline'
    # Uncomment the next line to enable the admin:
    # 'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
)

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        },
        'console': {
            'level': 'ERROR',
            'class': 'logging.StreamHandler'
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins', 'console'],
            'level': 'ERROR',
            'propagate': True,
        },
    }
}

TEST_RUNNER = 'tapiriik.testing.MongoDBTestRunner'

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_REPLICA_SET = None
MONGO_CLIENT_OPTIONS = {}
MONGO_FULL_WRITE_CONCERN = 1

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_CLIENT_OPTIONS = {}

WEB_ROOT = 'http://localhost:8000'

PP_WEBSCR = "https://www.sandbox.paypal.com/cgi-bin/webscr"
PP_BUTTON_ID = "XD6G9Z7VMRM3Q"
PP_RECEIVER_ID = "NR6NTNSRT7NDJ"
PAYMENT_AMOUNT = 2
PAYMENT_SYNC_DAYS = 365.25
PAYMENT_CURRENCY = "USD"


# Celebration mode config
# Because why not, I'm waiting for my account to get to the front of the sync queue.

CELEBRATION_MODES = {
    (
        datetime(day=21, month=6, year=datetime.now().year, hour=0, minute=0),
        datetime(day=21, month=6, year=datetime.now().year, hour=23, minute=59)
    ): {
        "Logo": "tapiriik-inuktitut.png",
        "Subtitle": "National Aboriginal Day",
        "TitleText": "Our Home on Native Land"
    }
}



# Hidden from regular signup
SOFT_LAUNCH_SERVICES = []

# Visibly disabled + excluded from synchronization
DISABLED_SERVICES = []

# Rejected by synchronization worker
REJECTED_SERVICES = []

# Services no longer available - will be removed across the site + excluded from sync.
WITHDRAWN_SERVICES = []

# Where to put per-user sync logs
USER_SYNC_LOGS = "./"

# Set at startup
SITE_VER = "unknown"

# Cache lots of stuff to make local debugging faster
AGGRESSIVE_CACHE = True

# Diagnostics auth, None = no auth
DIAG_AUTH_TOTP_SECRET = DIAG_AUTH_PASSWORD = None

SPORTTRACKS_OPENFIT_ENDPOINT = "https://api.sporttracks.mobi/api/v2"

EMAIL_BACKEND = 'django.core.mail.backends.filebased.EmailBackend'
EMAIL_FILE_PATH = './sent_emails'

WORKER_INDEX = int(os.environ.get("TAPIRIIK_WORKER_INDEX", 0))

# Used for distributing outgoing calls across multiple interfaces

HTTP_SOURCE_ADDR = "0.0.0.0"

RABBITMQ_BROKER_URL = os.getenv("RABBITMQ_BROKER_URL", "amqp://guest@localhost//")

RABBITMQ_USER_QUEUE_STATS_URL = "http://guest:guest@localhost:15672/api/queues/%2F/tapiriik-users?lengths_age=3600&lengths_incr=60&msg_rates_age=3600&msg_rates_incr=60"

GARMIN_CONNECT_USER_WATCH_ACCOUNTS = {}

from .local_settings import *
