# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/15 15:41
# Function:  计算每个气源的占比
from concurrent.futures import as_completed, ProcessPoolExecutor
from copy import deepcopy
from app.topo_generator.cycle_correction import cycle_correction
import networkx as nx

from common.log_utils import get_logger
from conf.const import MAX_WORKERS, ZERO_W_VALUE
from conf.constant import SIM_NODE_TYPE_MAP_REVERSE, NODE_TYPE_MAP_REVERSE

logger = get_logger(__name__)


class CalcGasSourceRatioAlg:
    def execute(self, user_id, pipe_result, gas_compos_heat):
        columns_name = ["gis_id", "flow_rate_vm", "ts", "calc_ts", "w_m_s"]
        pipe_result = cycle_correction(pipe_result)
        graph = nx.from_pandas_edgelist(pipe_result, edge_attr=columns_name, create_using=nx.DiGraph)
        for node in nx.topological_sort(graph):
            self.calc_gas_source_compos_node(graph, node, gas_compos_heat)
            self.calc_gas_source_compos_pipe(graph, node)
        ratio_lt, heat_lt = self.traverse_path_node_info(graph, gas_compos_heat)
        ret_dt = {"userId": user_id, "ratio_lt": ratio_lt, 'heat_lt': heat_lt}
        return ret_dt

    def calc_gas_source_compos_node(self, graph, curr_node, gas_heat):
        """
        curr_node: 计算当前节点的气源占比
        gas_flag: 当前节点是否是气源点
        """
        curr_node_info_dt = {"ts": "", "calc_ts": "", 'gas_ratio': {}, 'gas_compos': {}}
        gas_heat_dt = gas_heat.get(curr_node, None)
        if gas_heat_dt:  # 有热值表示是气源
            curr_node_info_dt['ts'] = gas_heat_dt['ts']  # 气源
            curr_node_info_dt['calc_ts'] = gas_heat_dt['calc_ts']
            curr_node_info_dt['gas_ratio'] = {curr_node: 1.0}  # 气源比率
            curr_node_info_dt['gas_compos'] = gas_heat_dt['gas_compos']
        else:  # 中间节点
            # 获取当前节点之前的节点id
            pre_nodes = list(graph.predecessors(curr_node))
            q_all = 0
            for node in pre_nodes:
                curr_w = graph[node][curr_node]['w_m_s']
                if -ZERO_W_VALUE <= curr_w <= ZERO_W_VALUE:  # 当前流量为0 直接退出本次循环
                    logger.debug(f'pipe: [{node}, {curr_node}]w is 0')
                    continue

                pre_node_ratio = graph.nodes[node]['gas_ratio']
                if not pre_node_ratio:
                    logger.warning(
                        f"pipe: {graph[node][curr_node]['gis_id']} w isn't 0, but source: {node} gas ratio is null")
                    continue

                pre_node_compos = graph.nodes[node]['gas_compos']
                if not pre_node_compos:
                    logger.warning(
                        f"pipe: {graph[node][curr_node]['gis_id']} w is 0, but source: {node} gas compose is null")
                    continue

                curr_flow = graph[node][curr_node]['flow_rate_vm']
                q_all += curr_flow
                # 更新之前的气源以及气组成分比率
                for key, value in deepcopy(curr_node_info_dt['gas_ratio']).items():
                    gas_source_percent = value * (1 - curr_flow / q_all)
                    curr_node_info_dt['gas_ratio'][key] = gas_source_percent
                for key, value in graph.nodes[node]['gas_ratio'].items():
                    gas_source_percent = graph.nodes[node]['gas_ratio'][key] * (curr_flow / q_all)
                    curr_node_info_dt['gas_ratio'][key] = curr_node_info_dt['gas_ratio'].get(key,
                                                                                             0) + gas_source_percent

                # 更新气源组分比率
                for key, value in deepcopy(curr_node_info_dt['gas_compos']).items():
                    gas_compos_percent = value * (1 - curr_flow / q_all)
                    curr_node_info_dt['gas_compos'][key] = gas_compos_percent
                for key, value in graph.nodes[node]['gas_compos'].items():
                    gas_compos_percent = graph.nodes[node]['gas_compos'][key] * (curr_flow / q_all)
                    curr_node_info_dt['gas_compos'][key] = curr_node_info_dt['gas_compos'].get(key,
                                                                                               0) + gas_compos_percent

                curr_node_info_dt['ts'] = graph[node][curr_node]['ts'].strftime("%Y-%m-%d %H:%M:%S")
                curr_node_info_dt['calc_ts'] = graph[node][curr_node]['calc_ts'].strftime("%Y-%m-%d %H:%M:%S")

        # 更新气源组分与气体组分到图节点
        self.update_graph_node_attr(graph, curr_node, curr_node_info_dt)

    def calc_gas_source_compos_pipe(self, graph, curr_node):
        """ 向后遍历计算管道的气源和气组 """
        attr_values = {'gas_ratio': graph.nodes[curr_node]['gas_ratio'],
                       'gas_compos': graph.nodes[curr_node]['gas_compos']}
        for node in graph.successors(curr_node):
            curr_w = graph[curr_node][node]['w_m_s']
            if -ZERO_W_VALUE <= curr_w <= ZERO_W_VALUE:
                attr_values = {'gas_ratio': {}, 'gas_compos': {}}
            self.update_graph_pipe_attr(graph, curr_node, node, attr_values)

    @staticmethod
    def update_graph_node_attr(graph, node, attrs):
        """ 更新属性到图 """
        for attr_name, attr_value in attrs.items():
            graph.nodes[node][attr_name] = attr_value

    @staticmethod
    def update_graph_pipe_attr(graph, snode, enode, attrs):
        for attr_name, attr_value in attrs.items():
            graph[snode][enode][attr_name] = attr_value

    @staticmethod
    def traverse_path_node_info(subgraph, gas_compos_heat):
        ratio_lt = []
        heat_lt = []
        for node_info in subgraph.nodes.data():
            node_id = node_info[0]
            node_ts = node_info[1]['ts']
            if not node_ts:
                logger.warning(f"node: {node_id} gas_compos and gas_ratio is null, skip it")
                continue
            gas_ratio = node_info[1]['gas_ratio']
            # node_calc_ts = node_info[1]['calc_ts']
            node_heat_value = CalcGasSourceRatioAlg.calc_heat_value(gas_ratio, gas_compos_heat)
            gas_compose = node_info[1]['gas_compos']
            node_kj_dt = {'ts': node_ts,  # 数据库中的ts
                          # 'calc_ts': node_calc_ts,  # 真实计算的ts
                          'kj': node_heat_value,  # 热值
                          "node_code": node_id,  # 节点ID
                          'node_type': "600000",  # 节点类型 待后续更新
                          'simulate_node_type': "800000"  # 仿真节点类型 待后续更新
                          }
            node_kj_dt.update(gas_compose)
            heat_lt.append(node_kj_dt)
            for gas_id, ratio in gas_ratio.items():
                node_ratio_dt = {'ts': node_ts,
                                 # 'calc_ts': node_calc_ts,
                                 'gas_ratio': ratio,  # 当前所在气源的占比
                                 'source_code': "",  # 节点类型没有source_code
                                 'target_code': "",  # 节点类型没有target_code
                                 'node_code': node_id,
                                 'gas_code': gas_id,  # 气源ID
                                 "node_type": "600000",  # 待后续更新
                                 "simulate_node_type": "800000"  # 待后续更新
                                 }
                ratio_lt.append(node_ratio_dt)

            for next_node in subgraph.successors(node_id):
                pipe_id = subgraph[node_id][next_node]['gis_id']
                pipe_ts = subgraph[node_id][next_node]['ts'].strftime("%Y-%m-%d %H:%M:%S")
                # pipe_calc_ts = subgraph[node_id][next_node]['calc_ts'].strftime("%Y-%m-%d %H:%M:%S")
                gas_ratio = subgraph[node_id][next_node]['gas_ratio']
                gas_compose = subgraph[node_id][next_node]['gas_compos']
                if not (gas_ratio and gas_compose):
                    continue
                pipe_heat_value = CalcGasSourceRatioAlg.calc_heat_value(gas_ratio, gas_compos_heat)
                pipe_kj_dt = {'ts': pipe_ts,
                              # 'calc_ts': pipe_calc_ts,
                              'kj': pipe_heat_value,
                              "node_code": pipe_id,
                              'node_type': NODE_TYPE_MAP_REVERSE['管段'],
                              'simulate_node_type': SIM_NODE_TYPE_MAP_REVERSE['管段']
                              }
                pipe_kj_dt.update(gas_compose)
                heat_lt.append(pipe_kj_dt)
                for gas_id, ratio in gas_ratio.items():
                    pipe_ratio_dt = {'ts': pipe_ts,
                                     # 'calc_ts': pipe_calc_ts,
                                     'gas_ratio': ratio,
                                     'source_code': node_id,
                                     'target_code': next_node,
                                     'node_code': pipe_id,
                                     'gas_code': gas_id,  # 气源ID
                                     "node_type": NODE_TYPE_MAP_REVERSE['管段'],
                                     "simulate_node_type": SIM_NODE_TYPE_MAP_REVERSE['管段']
                                     }
                    ratio_lt.append(pipe_ratio_dt)
        return ratio_lt, heat_lt

    @staticmethod
    def calc_heat_value(gas_ratio, gas_heat):
        heat_value = 0
        for node_id, ratio in gas_ratio.items():
            heat_value += gas_heat[node_id]['kj'] * ratio
        return heat_value


class CalcGasSourceRatio:
    def __init__(self, pipe_df_dt):
        self.pipe_df_dt = pipe_df_dt

    def execute(self):
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
            tasks = [
                pool.submit(CalcGasSourceRatioAlg().execute, elem_dt['user_id'], elem_dt['pipe_result'],
                            elem_dt['gas_heat_compose'])
                for elem_dt in self.pipe_df_dt]
            ret_lt = []
            for task in as_completed(tasks):
                ret_lt.append(task.result())
        return ret_lt
