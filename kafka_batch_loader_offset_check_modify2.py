#!/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author pengxiaolong@sensorsdata.cn
check && modify kafka and batch_loader offset tools
"""
import argparse
import shlex
import datetime
import os
import sys
import subprocess
import logging
import pymysql
import getpass


# pycommon
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../pycommon')))
import utils.sa_utils
from utils.sa_utils import SAMysql
import utils.shell_wrapper

logger = utils.sa_utils.init_tool_logger('kafka_batch_loader_offset_check_modify')

'''Get kafka old offset'''
def get_kafka_old_offset(topic, kafka_broker, partition_count):

    kafka_old_offset = {}
    #kafka_new_offset = {}

    try:
        #Get kafka offset through kafka module
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
        # Get kafka offset through confluent_kafka module
        from confluent_kafka import TopicPartition, Consumer, KafkaException
        from confluent_kafka.admin import AdminClient

        conf = {'bootstrap.servers': kafka_broker, 'session.timeout.ms': 6000}
        admin_client = AdminClient(conf)
        consumer_client = Consumer(conf)

        md = admin_client.list_topics(timeout=10)
        for t in iter(md.topics.values()):
            if str(t) == topic:
                for p in iter(t.partitions.values()):
                    td = TopicPartition(str(t), p.id)
                    oldest_offset, newest_offset  = consumer_client.get_watermark_offsets(td)
                    kafka_old_offset[p.id] = oldest_offset
                    #kafka_new_offset[p.id] = newest_offset
    except ImportError:
        for partition_id in range(partition_count):
            command = 'kafka-run-class kafka.tools.GetOffsetShell --topic {} --broker-list {} --time -2 --partition {}'.format(
                topic, kafka_broker, partition_id)
            #args = shlex.split(command)
            #process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #output = '{}'.format(process.stdout.read().decode(encoding='UTF-8'))
            output = utils.shell_wrapper.check_output(command)
            offset = output.split(':')[2]
            kafka_old_offset[partition_id] = int(offset)

    return kafka_old_offset

def get_batch_loader_offset(partition_count):
    ''' Get batch_loader saved offset '''
    batch_loader_offset = {}

    for partition_id in range(partition_count):
        sql = """SELECT end_offset FROM batch_loader_kafka_progress WHERE process_partition IN (
                 SELECT MAX(process_partition) FROM batch_loader_kafka_progress WHERE
                 kafka_partition_id = {}) AND kafka_partition_id= {} """.format(partition_id, partition_id)
        result = utils.sa_utils.SAMysql.query(sql, logger.debug)

        # Get bl offset
        for record in result:
            bl_offset = record['end_offset']
            batch_loader_offset[partition_id] = int(bl_offset)
    return batch_loader_offset

def check_bl_offset_kafka_old_offset(partition_count, kafka_old_offset, batch_loader_offset):
    ret = False
    for partition_id in range(partition_count):
        if kafka_old_offset[partition_id] <=  batch_loader_offset[partition_id]:
            logger.info('partition_id: {}  kafka_old_offset:{} is less than or equal to batch_loader_offset:{}, no need to modify'.format(
                        partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
        else:
            logger.error('partition_id: {}  kafka_old_offset:{} is greater than batch_loader_offset:{}, you need to modify'.format(
                        partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
            ret = True
    return ret

def modify_batch_loader_offset(partition_count, kafka_old_offset, batch_loader_offset):
    #compare batch_loader offset to kafka_old_offset
    for partition_id in range(partition_count):
        if kafka_old_offset[partition_id] <=  batch_loader_offset[partition_id]:
            logger.info('partition_id: {}  kafka_old_offset:{} is less than or equal to batch_loader_offset:{}, no need to modify'.format(
                        partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
        else:
            logger.error('partition_id: {}  kafka_old_offset:{} is greater than batch_loader_offset:{}, you need to modify'.format(
                        partition_id, kafka_old_offset[partition_id], batch_loader_offset[partition_id]))
            # modify mysql metadata from batch_loader_kafka_progress
            with SAMysql()  as cursor:
                sql = """UPDATE batch_loader_kafka_progress SET  end_offset={}  WHERE process_partition = (
                     SELECT* FROM (SELECT MAX(process_partition) FROM batch_loader_kafka_progress
                     WHERE kafka_partition_id = {}) AS tmpCol) AND kafka_partition_id={}""".format(
                     kafka_old_offset[partition_id], partition_id, partition_id)
                logger.info(sql)
                cursor.execute(sql)
                cursor.execute('commit')

def main(kafka_broker, topic_name, partition_count, args):
    '''
      compare the offset between batch_loader  and kafka
    '''
    kafka_old_offset = get_kafka_old_offset(topic_name, kafka_broker, partition_count)

    batch_loader_offset = get_batch_loader_offset(partition_count)
    '''offset check'''
    #test data
    #kafka_old_offset = {0: 60000000, 1: 80000000, 2: 80000000}
    #kafka_old_offset = {0: 60648554, 1: 72501826, 2: 73665446}
    print("args.modify:", args.modify)
    ret = check_bl_offset_kafka_old_offset(partition_count, kafka_old_offset, batch_loader_offset)
    if args.modify and ret:
        '''stop extractor && batch_loader modules'''
        logger.info("(1/3) stop extractor && batch_loader modules ...")
        cmd = 'sa_admin stop -m extractor'
        utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
        cmd = 'sa_admin stop -m batch_loader'
        utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
        logger.info("(2/3) modify mysql batch_loader_kafka_progress offset ...")
        modify_batch_loader_offset(partition_count, kafka_old_offset, batch_loader_offset)
        ''' start extractor && batch_loader modules'''
        logger.info("(3/3) start extractor && batch_loader modules ...")
        cmd = 'sa_admin start -m extractor'
        utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
        cmd = 'sa_admin start -m batch_loader'
        utils.shell_wrapper.check_call(cmd, logger.debug, action='modify_mysql_batch_loader_offset')
        logger.info("modify mysql metadata successfully ...")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group()
    #actions.add_argument('-c', '--check', action='store_true', dest='check', default=False, help='check offset')
    actions.add_argument('-m', '--modify', action='store_true', dest='modify', default=False, help='modify offset')
    args = parser.parse_args()

    #if len(sys.argv) < 2:
    #   parser.print_help()
     #   sys.exit(-1)

    '''Get customer type: standalone/cluster'''
    simplified_cluster = utils.sa_utils.SAZk.get_global_conf().get('simplified_cluster')
    kafka_broker_list = utils.sa_utils.SAZk.get_config('client','kafka')['broker_list']
    topic_name = utils.sa_utils.SAZk.get_config('client', 'kafka')['topic_name']
    partition_count = utils.sa_utils.SAZk.get_config('client','kafka')['partitions_count']
    if simplified_cluster:
        kafka_broker = ''.join(kafka_broker_list)
    else:
        kafka_broker = ','.join(kafka_broker_list)
    #main function
    main(kafka_broker, topic_name, partition_count, args)
