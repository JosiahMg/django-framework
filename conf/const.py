# -*- coding: utf-8 -*-
# Author: Lx
# Date: 2022/2/18 18:35
# 程序的所有可调变量
import os

MAX_WORKERS = os.cpu_count()  # 多线程执行最大线程数
ZERO_FLOW_VALUE = 1e-3  # kg/h
ZERO_W_VALUE = 2e-2  # m/s 小于0.5m/s的流速认为流速为0
FLOW_RATE_TYPE = "volume"  # volume: 体积流量   "mass"： 质量流量
