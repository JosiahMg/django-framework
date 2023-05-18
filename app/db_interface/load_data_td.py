# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/29 10:27
# Function: 从TD库读取稳定/瞬态计算结果以及热值

from app.db_interface.sql_sentence import SqlSentenceTD


class LoadDataFromTD:
    def __init__(self, project_id, batch_no, td_operator, dt_ts=None):
        self.project_id = project_id
        self.batch_no = batch_no
        self.td_op = td_operator
        self.dt_ts = dt_ts

    def load_compute_result(self):
        """ 获取batch no对应批号的所有computing result数据 """
        if self.dt_ts is None:
            sql = SqlSentenceTD.get_compute_result_by_batch_no(self.batch_no)
        else:
            sql = SqlSentenceTD.get_compute_result_by_batch_no_start_time(self.batch_no, self.dt_ts)
        compute_result_df = self.td_op.query_df(sql)
        return compute_result_df

    def load_ts_compute_result(self):
        """ 拉起batch no 批次号所有的时刻列表 """
        sql = SqlSentenceTD.get_ts_by_batch_no(self.batch_no)
        ts_lt = self.td_op.query_df(sql)
        return ts_lt

    def load_last_row_ts_compute_result(self, ts):
        """ 拉起batch no 批次号所有的时刻列表 """
        sql = SqlSentenceTD.get_last_row_ts_by_batch_no(self.batch_no, ts)
        ts_lt = self.td_op.query_df(sql)
        return ts_lt

    def load_compute_result_by_ts(self, ts):
        """ 拉取batch no批次中ts时刻的所有compute result数据 """
        sql = SqlSentenceTD.get_compute_result_by_batch_no_start_time(self.batch_no, ts)
        compute_result_ts_df = self.td_op.query_df(sql)
        return compute_result_ts_df

    def load_compute_result_by_ts_last_row(self, ts):
        """ 拉取batch no批次中ts时刻的所有compute result数据 """
        sql = SqlSentenceTD.get_compute_result_by_batch_no_start_time_last_row(self.batch_no, ts)
        compute_result_ts_df = self.td_op.query_df(sql)
        return compute_result_ts_df

    def load_compute_result_by_ts_gid(self, ts, gid):
        """ 拉取batch no批次中ts时刻的所有compute result数据 """
        sql = SqlSentenceTD.get_compute_result_by_ts_and_time(self.batch_no, ts, gid)
        compute_result_ts_df = self.td_op.query_df(sql)
        return compute_result_ts_df

    def load_compute_result_by_ts_gid_last_row(self, ts, gid):
        """ 拉取batch no批次中ts时刻的所有compute result数据 """
        sql = SqlSentenceTD.get_compute_result_by_ts_and_time_last_row(self.batch_no, ts, gid)
        compute_result_ts_df = self.td_op.query_df(sql)
        return compute_result_ts_df

    def load_gas_info_by_gid(self, gas_id):
        """ 依据gis_id读取气源的热值 """
        sql = SqlSentenceTD.get_gas_info_by_id(gas_id)
        gas_heat_df = self.td_op.query_df(sql)
        return gas_heat_df

    def load_gas_info_by_gid_ts(self, gas_id, ts):
        """ 依据gis_id以及时间读取气源的热值 """
        sql = SqlSentenceTD.get_gas_info_by_id_and_ts(gas_id, ts)
        gas_heat_df = self.td_op.query_df(sql)
        return gas_heat_df

    def load_gas_info_by_gid_ts_last_row(self, gas_id, ts):
        """ 依据gis_id以及时间读取气源的热值 """
        sql = SqlSentenceTD.get_gas_info_by_id_and_ts_last_row(gas_id, ts)
        gas_heat_df = self.td_op.query_df(sql)
        return gas_heat_df

    def load_gas_ts_by_gid(self, gas_id):
        """ 依据gis_id以及时间读取气源的热值 """
        sql = SqlSentenceTD.get_gas_ts_by_id_in_enn_iot_kj(gas_id)
        gas_ts_df = self.td_op.query_df(sql)
        return gas_ts_df

