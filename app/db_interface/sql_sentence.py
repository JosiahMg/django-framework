# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/16 11:11
# Function: SQL语句
from common.log_utils import get_logger

logger = get_logger(__name__)


class SqlSentencePG:

    @staticmethod
    def get_node_by_project_id(project_id):
        sql = """select gid, dno, name, type from dt_node_merge  where projectid = '{}' """.format(
            project_id)
        logger.info('pg sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_pipe_by_project_id(project_id):
        sql = """select gid, pipematerial, pipelength, pipediam, pressurerating,
                  wallthickness, source, target from dt_pipeline_merge
                  where projectid = '{}' """.format(project_id)
        logger.info('pg sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_node_type_by_project_id(project_code, topology_code):
        sql = f"""with t as (select code, topology_code, project_code
           from dt_node
           where op = '100000.000003' and topology_code = '{topology_code}')
            select a.code as gis_id, a.simulate_node_type as dno, node_type,a.name
            from dt_node a
            where not exists(select 1 from t where a.project_code = t.project_code and a.code = t.code and a.op!='100000.000002')
              and topology_code in ('{project_code}', '{topology_code}') and a.if_simulate=true;"""
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_pipe_length_by_project_id(project_code, topology_code):
        sql = f"""with t as (select code from dt_pipeline where op = '100000.000003' and topology_code = '{topology_code}')
            select a.code as gis_id,a.source_code,a.target_code,a.length as pipe_len_m
            from dt_pipeline a
            where not exists(select 1 from t where a.code = t.code and a.op!='100000.000002')
            and topology_code in ('{project_code}', '{topology_code}')
            and a.if_simulate = true;"""
        logger.info('sql: {}'.format(sql))
        return sql


class SqlSentenceTD:
    @staticmethod
    def get_compute_result_by_batch_no(batch_no):
        column_names = ['ts', 'volume_flow_rate', 'mass_flow_rate', 'flow_rate', 'node_code', 'simulate_node_type',
                        'source_code', 'target_code']
        column_name_str = ','.join(column_names)
        sql = f"select {column_name_str} from dt_compute_result where batch_no = '{batch_no}'"
        return sql

    @staticmethod
    def get_ts_by_batch_no(batch_no):
        sql = f"select distinct ts from dt_compute_result where batch_no = '{batch_no}'"
        return sql

    @staticmethod
    def get_last_row_ts_by_batch_no(batch_no, ts):
        sql = f"select last_row(ts) ts from dt_compute_result where batch_no = '{batch_no}' and ts <= '{ts}'"
        return sql

    @staticmethod
    def get_compute_result_by_batch_no_start_time(batch_no, start_time):
        column_names = ['ts', 'volume_flow_rate', 'mass_flow_rate', 'flow_rate', 'node_code', 'simulate_node_type',
                        'source_code', 'target_code']
        column_name_str = ','.join(column_names)
        sql = f"select {column_name_str} from dt_compute_result where batch_no = '{batch_no}' and ts = '{start_time}'"
        return sql

    @staticmethod
    def get_compute_result_by_batch_no_start_time_last_row(batch_no, start_time):
        sql = f"select last_row(ts, volume_flow_rate, mass_flow_rate, flow_rate, source_code, target_code), " \
              f"simulate_node_type " \
              f"from dt_compute_result where batch_no = '{batch_no}' and ts <= '{start_time}' group by node_code"
        return sql

    @staticmethod
    def get_compute_result_by_ts_and_time(batch_no, start_time, gis_id):
        column_names = ['ts', 'volume_flow_rate', 'mass_flow_rate', 'flow_rate', 'node_code', 'simulate_node_type',
                        'source_code', 'target_code']
        column_name_str = ','.join(column_names)
        sql = f"select {column_name_str} from dt_compute_result where batch_no = '{batch_no}' " \
              f"and ts = '{start_time}' and node_code = '{gis_id}'"
        return sql

    @staticmethod
    def get_compute_result_by_ts_and_time_last_row(batch_no, start_time, gis_id):
        sql = f"select last_row(ts, volume_flow_rate, mass_flow_rate, flow_rate, source_code, target_code), " \
              f"node_code, simulate_node_type " \
              f"from dt_compute_result where batch_no = '{batch_no}' and ts <= '{start_time}' and node_code = '{gis_id}'"
        return sql

    @staticmethod
    def get_gas_info_by_id(node_id):
        sql = f"select * from enn_iot_kj where node_code = '{node_id}'"
        return sql

    @staticmethod
    def get_gas_info_by_id_and_ts(node_id, ts):
        sql = f"select * from enn_iot_kj where node_code = '{node_id}' and ts = '{ts}'"
        return sql

    @staticmethod
    def get_gas_info_by_id_and_ts_last_row(node_id, ts):
        sql = f"select last_row(*), tenant_code, node_code, node_type, simulate_node_type from enn_iot_kj " \
              f"where node_code = '{node_id}' and ts <= '{ts}'"
        return sql

    @staticmethod
    def get_gas_ts_by_id_in_enn_iot_kj(node_id):
        sql = f"select ts from enn_iot_kj where node_code = '{node_id}'"
        return sql
