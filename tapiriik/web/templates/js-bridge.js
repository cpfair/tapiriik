{% load users %}
{% load displayutils %}
tapiriik.SiteVer = "{{ config.siteVer|slice:":7" }}";
tapiriik.ServiceInfo = {{ js_bridge_serviceinfo|safe }}
tapiriik.MinimumSyncInterval = {{ config.minimumSyncInterval }};
{% if user %}tapiriik.User = {
	ConnectedServicesCount: {{ user.ConnectedServices|length }},
	ID: "{{ user|dict_get:'_id' }}",
	Timezone: "{{ user|dict_get:'Timezone' }}",
	Substitute: {{ user.Substitute|lower }},
	AutoSyncActive: {{ user|has_active_payment|lower }},
	Config: {{ user.Config|json|safe }}
};
{% endif %}