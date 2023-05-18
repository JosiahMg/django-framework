# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/12/7 13:57
# Function: 溯源结果组织并保存到TD库
import os.path
import pandas as pd
from common.log_utils import get_logger
from conf.path_config import resource_dir

logger = get_logger(__name__)


class EnnComputingSourceRatio:
    """ 存放气源比率的数据结构 """

    def __init__(self, **kwargs):
        self.ts = kwargs['ts']  # TIMESTAMP
        self.source_ratio = kwargs['source_ratio']  # DOUBLE
        self.batch_no = kwargs['batch_no']  # VARCHAR
        self.node_node = kwargs['node_code']  # VARCHAR
        self.source_code = kwargs['source_code']  # VARCHAR
        self.node_type = kwargs['node_type']  # VARCHAR
        self.simulate_node_type = kwargs['simulate_node_type']  # VARCHAR


class EnnComputingKj:
    """ 存放气源热值的数据结构 """

    def __init__(self, **kwargs):
        self.ts = kwargs['ts']  # TIMESTAMP
        self.kj = kwargs['kj']  # DOUBLE
        self.ch4 = kwargs['ch4']  # DOUBLE
        self.c2h4 = kwargs['c2h4']  # DOUBLE
        self.c2h6 = kwargs['c2h6']  # DOUBLE
        self.c3h8 = kwargs['c3h8']  # DOUBLE
        self.n_c4h10 = kwargs['n_c4h10']  # DOUBLE
        self.i_c4h10 = kwargs['i_c4h10']  # DOUBLE
        self.c5h12 = kwargs['c5h12']  # DOUBLE
        self.i_c5h12 = kwargs['i_c5h12']  # DOUBLE
        self.n_c5h12 = kwargs['n_c5h12']  # DOUBLE
        self.c6 = kwargs['c6']  # DOUBLE
        self.n2 = kwargs['n2']  # DOUBLE
        self.o2 = kwargs['o2']  # DOUBLE
        self.h2s = kwargs['h2s']  # DOUBLE
        self.project_code = kwargs['project_code']  # VARCHAR
        self.batch_no = kwargs['batch_no']  # VARCHAR
        self.node_code = kwargs['node_code']  # VARCHAR
        self.node_type = kwargs['node_type']  # VARCHAR


class TraceResultCompose:
    def __init__(self, project_id, batch_no, start_time, trace_result_data):
        self.project_id = project_id
        self.batch_no = batch_no
        self.start_time = start_time
        self.data = trace_result_data  # 溯源数据

    def execute(self, gas_provide_df=None, node_df=None):
        """
        gas_provide_df: 每个节点到各个气源的供气时间
        node_df: 点表信息
        """
        ratio_df = pd.DataFrame([])
        kj_df = pd.DataFrame([])
        if isinstance(self.data, dict):
            self.data = [self.data]
        for data in self.data:
            ratio_df = pd.concat([ratio_df, pd.DataFrame(data['ratio_lt'])], axis=0, ignore_index=True)
            kj_df = pd.concat([kj_df, pd.DataFrame(data['heat_lt'])], axis=0, ignore_index=True)

        # 读取node点表信息 用于更新ratio_df中的node_type和simulate_node_type
        if node_df is None:
            node_path = os.path.join(resource_dir, self.project_id, self.batch_no, 'nodes.csv')
            node_df = pd.read_csv(node_path, encoding='utf-8')
            node_df['gis_id'] = node_df['gis_id'].astype(str)

        node_df.rename(columns={'dno': 'simulate_node_type', 'gis_id': 'node_code'}, inplace=True)
        # 判断点表是有重复的id
        duplicated_df = node_df[node_df['node_code'].duplicated()]
        if not duplicated_df.empty:
            node_id = duplicated_df['node_code'].to_list()
            logger.warning(f"node is duplicated: {node_id}")

        node_df.drop_duplicates(subset=['node_code'], inplace=True, ignore_index=True)
        node_df.set_index('node_code', inplace=True)

        # 更新点表信息的点表类型到ratio_df
        ratio_df.sort_values(by='ts', inplace=True, ascending=False)
        ratio_df.drop_duplicates(subset=['node_code', 'gas_code'], inplace=True, ignore_index=True)
        ratio_df.set_index('node_code', inplace=True)
        ratio_df.update(node_df)
        ratio_df.reset_index(drop=False, inplace=True)
        ratio_df['project_code'] = self.project_id
        ratio_df['batch_no'] = self.batch_no
        ratio_df['ts'] = self.start_time  # 更新ts为start_time
        if gas_provide_df is not None:  # 气源供气时间
            gas_provide_df['provide_gas_time'] = gas_provide_df['provide_gas_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            ratio_df = pd.merge(ratio_df, gas_provide_df, on=['node_code', 'gas_code'], how='left')

        # 更新点表信息的点表类型到kj_df
        kj_df.sort_values(by='ts', inplace=True, ascending=False)
        kj_df.drop_duplicates(subset=['node_code'], inplace=True, ignore_index=True)
        kj_df.set_index('node_code', inplace=True)
        kj_df.update(node_df)
        kj_df.reset_index(drop=False, inplace=True)
        kj_df['project_code'] = self.project_id
        kj_df['batch_no'] = self.batch_no
        kj_df['ts'] = self.start_time  # 更新ts为start_time

        return ratio_df, kj_df
