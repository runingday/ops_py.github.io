#!/bin/env python3
# -*- coding: UTF-8 -*-
"""
Copyring (c) 2018 SensorsData, Inc. All Rights Reserved
@author pengxiaolong@sensorsdata.cn
校验&修改kafka及batch_loader offset 工具
"""
import argparse
import shlex
import datetime
import os
import sys
import subprocess
import logging
import pymysql


# pycommon
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../pycommon')))
import utils.sa_utils
import utils.shell_wrapper

logger = utils.sa_utils.init_tool_logger('kafka_batch_loader_offset_check_modify')

def get_kafka_old_offset(topic, kafka_broker, partition_count):

    '''获取kafka 最旧的offset 用来跟后面的 batch_loader 所读取到的offset 做对比'''
    kafka_old_offset = {}
    kafka_new_offset = {}

    try:
        #使用kafka 库来获取的方式
        '''
        from kafka import SimpleClient
        from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
        from kafka.common import OffsetRequestPayload
        client = SimpleClient(broker_list)
        partitions = client.topic_partitions[topic]
        offset_requests = [OffsetRequestPayload(topic, p, -1, 1) for p in partitions.keys()]
        offsets_responses = client.send_offset_request(offset_requests)
        for r in offsets_responses:
            #print("partition = %s, offset = %s"%(r.partition, r.offsets[0]))
            kafka_old_offset[r.partition] = r.offsets[0]
       '''
        from confluent_kafka import TopicPartition, Consumer, KafkaException
        from confluent_kafka.admin import AdminClient

        conf = {'bootstrap.servers': kafka_broker, 'session.timeout.ms': 6000}
        try:
            admin_client = AdminClient(conf)
            consumer_client = Consumer(conf)

            md = admin_client.list_topics(timeout=10)
            for t in iter(md.topics.values()):
                if str(t) == topic:
                    for p in iter(t.partitions.values()):
                        td = TopicPartition(str(t), p.id)
                        oldest_offset, newest_offset  = consumer_client.get_watermark_offsets(td)
                        kafka_old_offset[p.id] = oldest_offset
                        kafka_new_offset[p.id] = newest_offset
        except KafkaException as e:
            logger.error("请检查kafka是否存活:%s" %e)
    except ImportError:
        for partition_id in range(partition_count):
            command = 'kafka-run-class kafka.tools.GetOffsetShell --topic %s --broker-list %s --time -2 --partition %d' %(topic, kafka_broker, partition_id)
            args = shlex.split(command)
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output = '{}'.format(process.stdout.read().decode(encoding='UTF-8'))
            offset = output.split(':')[2]
            kafka_old_offset[partition_id] = int(offset)

    return kafka_old_offset

def get_batch_loader_offset(partition_count):
    ''' 获取batch_loader 所保留的offset 位置 '''
    batch_loader_offset = {}

    for partition_id in range(partition_count):
        sql = "select end_offset from batch_loader_kafka_progress where process_partition in (select max(process_partition) from batch_loader_kafka_progress where kafka_partition_id = %d) and kafka_partition_id=%d" %(partition_id, partition_id)
        result = utils.sa_utils.SAMysql.query(sql, logger.debug)

        # 获得bl offset
        for record in result:
            bl_offset = record['end_offset']
            batch_loader_offset[partition_id] = int(bl_offset)
    return batch_loader_offset

def check_bl_offset_kafka_old_offset(partition_count, kafka_old_offset, batch_loader_offset):
    ret = None
    for partition_id in range(partition_count):
        if kafka_old_offset[partition_id] <=  batch_loader_offset[partition_id]:
            logger.info('partition_id: {}  kafka_old_offset:{} is less than or equal to batch_loader_offset:{}, no need to modify'.format(
                        partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
            ret = 0
        else:
            logger.error('partition_id: {}  kafka_old_offset:{} is greater than batch_loader_offset:{}, you need to modify'.format(
                        partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
            ret = 1
    return ret

def check_bl_offset_and_kafka_old_offset_modify(kafka_broker, topic_name, partition_count, **kargs):
    '''
      对比kafka old offset 与batch_loader 所读到的 offset 值
    '''
    kafka_old_offset = get_kafka_old_offset(topic_name, kafka_broker, partition_count)
    ### 测试数据

    #kafka_old_offset = {0: 70000000, 1: 80000000, 2: 80000000}
    #kafka_old_offset = {0: 60635564, 1: 72486555, 2: 73649957}
    batch_loader_offset = get_batch_loader_offset(partition_count)
    '''offset 校验'''
    if kargs['check']:
        ret = check_bl_offset_kafka_old_offset(partition_count, kafka_old_offset, batch_loader_offset)
    if kargs['modify']:
        ret = check_bl_offset_kafka_old_offset(partition_count, kafka_old_offset, batch_loader_offset)
        if ret:
            '''停止extractor 与 batch_loader模块'''
            logger.info("(1/3) 停止模块 ...")
            cmd = 'sa_admin stop -m extractor'
            utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
            cmd = 'sa_admin stop -m batch_loader'
            utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')

            logger.info("(2/3) 修改offset ...")
            ''' 修改前再次检测，防止不检测直接修改'''
            ''' 暂时没有 mysql update 的封装接口，所以才有了下面的代码 '''
            mysql_info = utils.sa_utils.SAZk.get_client_conf('mysql')
            param = {'user': mysql_info['user'], 'passwd': mysql_info['password'], 'charset': 'utf8'}
            master_jdbc_url = mysql_info['jdbc_url_list'][mysql_info['master_index']]
            param['db'] = master_jdbc_url.split('/')[-1]
            host_and_port = master_jdbc_url.split('/')[-2]
            param['host'], param['port'] = host_and_port.split(':')
            param['port'] = int(param['port'])
            # pymysql新版本不兼容旧版本
            # https://github.com/PyMySQL/PyMySQL/issues/590
            param['client_flag'] = pymysql.constants.CLIENT.MULTI_STATEMENTS
            mysql_con = pymysql.connect(**param)
            for partition_id in range(partition_count):
                if kafka_old_offset[partition_id] <=  batch_loader_offset[partition_id]:
                    logger.info('partition_id: {}  kafka_old_offset:{} is less than or equal to batch_loader_offset:{}, no need to modify'.format(
                                partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
                else:
                    logger.error('partition_id: {}  kafka_old_offset:{} is greater than batch_loader_offset:{}, you need to modify'.format(
                                partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
                    # 要执行的sql 语句
                    with mysql_con.cursor() as cursor:
                        sql = """UPDATE batch_loader_kafka_progress SET  end_offset={offset}  WHERE process_partition = (
                             SELECT* FROM (SELECT MAX(process_partition) FROM batch_loader_kafka_progress
                             WHERE kafka_partition_id = {id}) AS tmpCol) AND kafka_partition_id={id}""".format(
                        offset=kafka_old_offset[partition_id], id=partition_id)
                        logger.info(sql)
                        cursor.execute(sql)
                        mysql_con.commit()

            mysql_con.close()
            ''' 启动 extractor 与 batch_loader 模块'''
            logger.info("(3/3) 启动模块 ...")
            cmd = 'sa_admin start -m extractor'
            utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
            cmd = 'sa_admin start -m batch_loader'
            utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
            logger.info("修改元数据成功 ...")
        else:
            sys.exit(0)
        #    start_related_module()
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('-c', '--check', action='store_true', dest='check', default=False, help='check offset')
    actions.add_argument('-m', '--modify', action='store_true', dest='modify', default=False, help='modify offset')
    args = parser.parse_args()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)
    params = dict(vars(args))
    '''获取用户类型为单机还是集群'''
    customer_conf = utils.sa_utils.SAZk.get_global_conf().get('simplified_cluster')
    kafka_broker_list = utils.sa_utils.SAZk.get_config('client','kafka')['broker_list']
    topic_name = utils.sa_utils.SAZk.get_config('client', 'kafka')['topic_name']
    partition_count = utils.sa_utils.SAZk.get_config('client','kafka')['partitions_count']
    if customer_conf:
        kafka_broker = ''.join(kafka_broker_list)
    else:
        kafka_broker = ','.join(kafka_broker_list)
    #执行check 或modify
    check_bl_offset_and_kafka_old_offset_modify(kafka_broker, topic_name, partition_count, **params)
