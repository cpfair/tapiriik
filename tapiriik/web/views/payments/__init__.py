from tapiriik.settings import PP_WEBSCR, PP_RECEIVER_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY
from tapiriik.auth import Payments, User
from django.http import HttpResponse
from django.shortcuts import redirect, render
import requests

def payments_ipn(req):
	data = req.POST.dict()
	data["cmd"] = "_notify-validate"
	response = requests.post(PP_WEBSCR, data=data)
	if response.text != "VERIFIED":
		return HttpResponse(code=403)
	if req.POST["receiver_id"] != PP_RECEIVER_ID or req.POST["mc_gross"] != PAYMENT_AMOUNT or req.POST["mc_currency"] != PAYMENT_CURRENCY:
		return HttpResponse(code=403)
	if req.POST["payment_status"] != "Completed":
		return HttpResponse()
	payment = Payments.LogPayment(req.POST["txn_id"])
	user = User.Get(req.POST["custom"])
	User.AssociatePayment(user, payment)
	return HttpResponse()

def payments_return(req):
	if req.user is None or User.HasActivePayment(req.user):
		return redirect("/")
	return render("payments/return")
