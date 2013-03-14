from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.views.generic import TemplateView

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    url(r'^$', 'tapiriik.web.views.dashboard', name='dashboard'),

    url(r'^auth/redirect/(?P<service>.+)$', 'tapiriik.web.views.oauth.authredirect', {}, name='oauth_redirect', ),
    url(r'^auth/return/(?P<service>.+)$', 'tapiriik.web.views.oauth.authreturn', {}, name='oauth_return', ),
    url(r'^auth/deauth/(?P<service>.+)$', 'tapiriik.web.views.oauth.deauth', {}, name='oauth_deauth', ),
    url(r'^auth/login/(?P<service>.+)$', 'tapiriik.web.views.auth_login', {}, name='auth_simple', ),
    url(r'^auth/login-ajax/(?P<service>.+)$', 'tapiriik.web.views.auth_login_ajax', {}, name='auth_simple_ajax', ),
    url(r'^auth/disconnect/(?P<service>.+)$', 'tapiriik.web.views.auth_disconnect', {}, name='auth_disconnect', ),
    url(r'^auth/disconnect-ajax/(?P<service>.+)$', 'tapiriik.web.views.auth_disconnect_ajax', {}, name='auth_disconnect_ajax', ),

    url(r'^config/save/(?P<service>)?$', 'tapiriik.web.views.config_save', {}, name='config_save', ),

    url(r'^dropbox/browse-ajax/?$', 'tapiriik.web.views.dropbox.browse', {}, name='dropbox_browse_ajax', ),
    url(r'^dropbox/browse-ajax/(?P<path>.+)?$', 'tapiriik.web.views.dropbox.browse', {}, name='dropbox_browse_ajax', ),

    url(r'^sync/status$', 'tapiriik.web.views.sync_status', {}, name='sync_status'),
    url(r'^sync/schedule/now$', 'tapiriik.web.views.sync_schedule_immediate', {}, name='sync_schedule_immediate'),

    url(r'^diagnostics/$', 'tapiriik.web.views.diag_dashboard', {}, name='diagnostics_dashboard'),
    url(r'^diagnostics/user/(?P<user>.+)$', 'tapiriik.web.views.diag_user', {}, name='diagnostics_user'),
    url(r'^diagnostics/login$', 'tapiriik.web.views.diag_login', {}, name='diagnostics_login'),

    url(r'^supported-activities$', 'tapiriik.web.views.supported_activities', {}, name='supported_activities'),

    url(r'^faq$', TemplateView.as_view(template_name='static/faq.html'), name='faq'),
    url(r'^privacy$', TemplateView.as_view(template_name='static/privacy.html'), name='privacy'),
    url(r'^credits$', TemplateView.as_view(template_name='static/credits.html'), name='credits'),
    # Examples:
    # url(r'^$', 'tapiriik.views.home', name='home'),
    # url(r'^tapiriik/', include('tapiriik.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)

urlpatterns += staticfiles_urlpatterns()
