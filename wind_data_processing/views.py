from django.http import HttpResponse

from . import tasks

# Create your views here.
def home(request):
    print('celery triggerd ')
    # result = tasks.sharedtask.delay()
    # print(result)
    # print(result.get())
    return HttpResponse('celery triggerd ')
