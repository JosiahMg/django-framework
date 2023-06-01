# -*- coding: utf-8 -*-
import requests

from common.log_utils import get_logger

logger = get_logger(__name__)


def request_post(url, param):
    """
    :param url: http地址
    :param param: body体, json格式
    :return:
    """
    try:
        # headers = {"Content-Type": "application/json; charset=utf-8"}
        headers = {'charset': 'utf-8', 'application': 'json'}
        response = requests.post(url=url, data=param, headers=headers)
        return response.json()
    except Exception as e:
        logger.error(f"connect url: {url} failed: {e}")
        raise ConnectionError(f"connect url: {url} failed: {e}")
