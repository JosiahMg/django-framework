# -*- coding: utf-8 -*-
# Author: Lx
# Date: 2021/3/16 18:20

from django.http import HttpResponse
from django.urls import path

from api.views import trace_source


def index(request):
    return HttpResponse("Hello, world. You're at Django Framework.")


urlpatterns = [
    path('', index, name='index'),
    path('trace-source', trace_source.trace_source),
]
