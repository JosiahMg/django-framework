# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/12 10:36
# Function:
import pandas as pd

from app.db_interface.load_data_td import LoadDataFromTD
from app.db_interface.save_trace_result_td import InsertEnnRatioKjToTd
from app.trace_source.gas_source_ratio import CalcGasSourceRatio, CalcGasSourceRatioAlg
from app.trace_source.trace_result_compose import TraceResultCompose
from common.log_utils import get_logger
from database.operate_tdengine import OperateTD

logger = get_logger(__name__)


class TraceSource:
    def __init__(self, project_id, topo_id, batch_no, start_time, calc_type):
        self.project_id = project_id
        self.topo_id = topo_id
        self.batch_no = batch_no
        self.start_time = start_time
        self.calc_type = calc_type
        self.td_op = OperateTD()

    def execute(self):
        # Step1: 取数据 管道流量 流速 管长 以及节点类型等信息
        ratio_df = pd.DataFrame([])
        kj_df = pd.DataFrame([])
        # Step2: 保存溯源结果到TD库
        InsertEnnRatioKjToTd(self.td_op).execute(ratio_df, kj_df)
