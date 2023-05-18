# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2023/2/25 10:53
# Function: 追踪路径 给定用户ID 获取该用户到所有气源点的路径


import json
import traceback
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from api.views.common.date_encoder import DateEncoder
from api.views.common.status import Status, INIT_STATUS, STATUS
from common.log_utils import get_logger
from api.service.service_trace_path import ServiceSourcePath

logger = get_logger(__name__)


def post_method_proc(request, context):
    try:
        logger.debug('receive trace path post method request')
        params_dict = request.POST
        if request.content_type == 'application/json':
            params_dict = json.loads(request.body if request.body else '{}')
        data = ServiceSourcePath(params_dict).execute()
        context.update(data)
    except Exception as e:
        message = f'计算路径失败, 错误原因: {e}'
        logger.error(traceback.format_exc())
        STATUS[Status.OTHER_ERROR.name]['message'] = message
        context.update(STATUS[Status.OTHER_ERROR.name])

    return HttpResponse(json.dumps(context, cls=DateEncoder, ensure_ascii=False),
                        content_type="application/json; charset=utf-8")


def other_method_proc(request, context):
    logger.debug('receive trace path get method request')
    context.update(STATUS[Status.BAD_GET.name])
    return HttpResponse(json.dumps(context, cls=DateEncoder, ensure_ascii=False),
                        content_type="application/json; charset=utf-8")


@csrf_exempt
def trace_path(request):
    """ 从前推送同济计算使用的边界值 """
    context = INIT_STATUS.copy()
    if request.method == 'POST':
        return post_method_proc(request, context)
    else:
        return other_method_proc(request, context)


