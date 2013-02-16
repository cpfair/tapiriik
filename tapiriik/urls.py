from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.views.generic import TemplateView

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

    url(r'^sync/status$', 'tapiriik.web.views.sync_status', {}, name='sync_status'),
    url(r'^sync/schedule/now$', 'tapiriik.web.views.sync_schedule_immediate', {}, name='sync_schedule_immediate'),

    url(r'^faq$', TemplateView.as_view(template_name='static/faq.html'), name='faq'),
    url(r'^privacy$', TemplateView.as_view(template_name='static/privacy.html'), name='privacy'),
    # Examples:
    # url(r'^$', 'tapiriik.views.home', name='home'),
    # url(r'^tapiriik/', include('tapiriik.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)

urlpatterns += staticfiles_urlpatterns()
