from django.template.loader import get_template
from django.template import Context
from django.core.mail import EmailMultiAlternatives
from django.conf import settings

def generate_message_from_template(template, context):
	context["STATIC_URL"] = settings.STATIC_URL
	# Mandrill is set up to inline the CSS and generate a plaintext copy.
	html_message = get_template(template).render(Context(context)).strip()
	context["plaintext"] = True
	plaintext_message = get_template(template).render(Context(context)).strip()
	return html_message, plaintext_message

def send_email(recipient_list, subject, html_message, plaintext_message=None):
	if type(recipient_list) is not list:
		recipient_list = [recipient_list]

	email = EmailMultiAlternatives(subject=subject, body=plaintext_message, from_email="tapiriik <mailer@tapiriik.com>", to=recipient_list, headers={"Reply-To": "contact@tapiriik.com"})
	email.attach_alternative(html_message, "text/html")
	email.send()