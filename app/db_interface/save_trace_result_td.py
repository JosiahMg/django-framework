# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/29 10:31
# Function: 写溯源结果到TD库

from common.log_utils import get_logger
from conf.const import MAX_WORKERS

logger = get_logger(__name__)


class InsertEnnRatioKjToTd:
    def __init__(self, td_operator):
        self.td_opera = td_operator
        self.ratio_tb_name = "dt_compute_source_ratio"
        self.ratio_tag = ['project_code', 'batch_no', 'node_code', 'gas_code', 'simulate_node_type', 'node_type']
        self.kj_tb_name = 'dt_compute_kj'
        self.kj_tag = ['project_code', 'batch_no', 'node_code', 'node_type', 'simulate_node_type']

    def execute(self, ratio_df, kj_df):
        self.td_opera.batch_insert_by_tags(ratio_df, self.ratio_tb_name, self.ratio_tag,
                                           ['batch_no', 'node_code', 'gas_code'], max_workers=MAX_WORKERS)
        logger.info("write source ratio into td engine success")

        self.td_opera.batch_insert_by_tags(kj_df, self.kj_tb_name, self.kj_tag,
                                           ['batch_no', 'node_code'], max_workers=MAX_WORKERS)
        logger.info("write source kj into td engine success")
