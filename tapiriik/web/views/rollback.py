from tapiriik.services.rollback import RollbackTask
from django.http import HttpResponse
from django.views.decorators.http import require_GET
from django.shortcuts import redirect, render

def account_rollback_initiate(req):
    if not req.user:
        return HttpResponse(status=403)

    task = RollbackTask.Create(req.user)

    return HttpResponse(task.json())

def account_rollback_status(req, task_id):
    if not req.user:
        return HttpResponse(status=403)
    task = RollbackTask.Get(task_id)

    if not task:
        return HttpResponse(status=404)

    if req.method == 'POST':
        task.Schedule()
    return HttpResponse(task.json())

def rollback_dashboard(req):
    if not req.user:
        return redirect('/')
    return render(req, "rollback.html")