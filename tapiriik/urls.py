from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    url(r'^$', 'tapiriik.web.views.dashboard', name='dashboard'),
    url(r'^auth/return/(?P<service>.+)$', 'tapiriik.web.views.oauth.authreturn', {}, name='oauth_return', ),
    url(r'^auth/login/(?P<service>.+)$', 'tapiriik.web.views.auth_login', {}, name='auth_simple', ),
    url(r'^auth/login-ajax/(?P<service>.+)$', 'tapiriik.web.views.auth_login_ajax', {}, name='auth_simple_ajax', ),
    url(r'^auth/disconnect/(?P<service>.+)$', 'tapiriik.web.views.auth_disconnect', {}, name='auth_disconnect', ),
    url(r'^auth/disconnect-ajax/(?P<service>.+)$', 'tapiriik.web.views.auth_disconnect_ajax', {}, name='auth_disconnect_ajax', ),
    # Examples:
    # url(r'^$', 'tapiriik.views.home', name='home'),
    # url(r'^tapiriik/', include('tapiriik.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)

urlpatterns += staticfiles_urlpatterns()
