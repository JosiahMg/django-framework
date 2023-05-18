# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/29 10:26
# Function: 从PG库读取气源点和工商户点的ID号以及管道的管道长度信息
import pandas as pd
from database.postgresql import PostgreOp
from app.db_interface.sql_sentence import SqlSentencePG
from common.log_utils import get_logger


logger = get_logger(__name__)


class FetchNodeDataPG:
    def __init__(self, project_id, topo_id):
        self.project_id = project_id
        self.topo_id = topo_id
        self.pg_op = PostgreOp()

    def fetch_node_data(self):
        sql = SqlSentencePG.get_node_type_by_project_id(self.project_id, self.topo_id)
        data = self.pg_op.query(sql)
        data_df = pd.DataFrame(data)
        if data_df.empty:
            logger.error(f"get project: {self.project_id}, topo_id: {self.topo_id} node info failed from postgresql")
            raise Exception(f"从PG库获取项目: {self.project_id}, topo_id: {self.topo_id}的点表数据失败")
        return data_df

    def fetch_pipe_data(self):
        sql = SqlSentencePG.get_pipe_length_by_project_id(self.project_id, self.topo_id)
        data = self.pg_op.query(sql)
        data_df = pd.DataFrame(data)
        if data_df.empty:
            logger.error(f"get project: {self.project_id}, topo_id: {self.topo_id} pipe info failed from postgresql")
            raise Exception(f"从PG库获取项目: {self.project_id}, topo_id: {self.topo_id}的线表数据失败")
        return data_df

    def __del__(self):
        self.pg_op.close()
        logger.info('close postgres')
