# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2023/2/15 11:32
# Function: 验证路径有效性
import os
import networkx as nx
import pandas as pd
from conf.path_config import data_dir
from app.db_interface.load_data_td import LoadDataFromTD
from common.log_utils import get_logger
from database.operate_tdengine import OperateTD

logger = get_logger(__name__)


class VerifyUserPath:
    def __init__(self, project_id, batch_no, dt_ts, direction='N-Di'):
        self.td_op = OperateTD()
        self.project_id = project_id
        self.batch_no = batch_no
        self.direction = direction
        self.dt_ts = dt_ts
        self.pipe_result_df = None  # compute result数据
        self.dir_graph = None  # 有向图
        self.gas_ids = None

    def load_compute_result(self):
        td_driver = LoadDataFromTD(self.project_id, self.batch_no, self.td_op, self.dt_ts)
        self.pipe_result_df = td_driver.load_compute_result()

    def make_direct_graph(self):
        if self.pipe_result_df is not None:
            if self.direction == 'N-Di':
                self.dir_graph = nx.from_pandas_edgelist(self.pipe_result_df,
                                                         source="source_code",
                                                         target="target_code",
                                                         create_using=nx.Graph)
            else:
                self.dir_graph = nx.from_pandas_edgelist(self.pipe_result_df,
                                                         source="source_code",
                                                         target="target_code",
                                                         create_using=nx.DiGraph)
        else:
            raise Exception("compute result is null")

    def load_gas_ids(self):
        if self.pipe_result_df is not None:
            self.gas_ids = self.pipe_result_df.loc[
                self.pipe_result_df['simulate_node_type'] == '800000.000001', 'node_code'].values
            logger.info(f'gas_ids: {self.gas_ids}')

    def get_path_each_gas(self, source, target):
        for i, node_path in enumerate(nx.all_simple_paths(self.dir_graph, source=source, target=target), 1):
            logger.info(f"***** It is {i} path for {source} to {target} *****")
            pipe_path_df = pd.DataFrame([])
            for j, node in enumerate(node_path[1:]):
                pre_node = node_path[j]
                condition = ((self.pipe_result_df['source_code'] == node) & (
                        self.pipe_result_df['target_code'] == pre_node)) | (
                                    (self.pipe_result_df['source_code'] == pre_node) & (
                                    self.pipe_result_df['target_code'] == node))
                pipe_info = self.pipe_result_df.loc[
                    condition, ['node_code', 'source_code', 'target_code', 'volume_flow_rate', 'mass_flow_rate',
                                'flow_rate']]
                pipe_path_df = pd.concat([pipe_path_df, pipe_info], axis=0)
            filename = f"{target}_{source}_path_{i}.csv"
            filepath = os.path.join(data_dir, 'path', self.batch_no)
            if not os.path.exists(filepath):
                os.makedirs(filepath)
            pipe_path_df.to_csv(os.path.join(filepath, filename), index=False)

    def get_all_path(self, source, target):
        if source is None:
            gas_ids = self.gas_ids
        else:
            gas_ids = [source]

        for gas_id in gas_ids:
            logger.info(f"********** way finding gas id: {gas_id} to {target} **********")
            self.get_path_each_gas(gas_id, target)

    def execute(self, user_id=None, gas_id=None):
        self.load_compute_result()
        self.make_direct_graph()
        self.load_gas_ids()
        self.get_all_path(gas_id, user_id)


if __name__ == "__main__":
    ver = VerifyUserPath('dt_dg', '100_20221221', '2023-03-08 10:00:00', 'N-Di')
    ver.execute(user_id='120010', gas_id=None)
