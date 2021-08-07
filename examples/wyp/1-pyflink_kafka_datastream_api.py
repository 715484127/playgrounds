import json
import os
from datetime import timedelta

import pyflink
from pyflink.common import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import BasicTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
# 测试 kafka 配置
from pyflink.datastream.connectors import FlinkKafkaConsumer

TEST_KAFKA_SERVERS = "localhost:9092"
TEST_KAFKA_TOPIC = "pyflink_source"
TEST_GROUP_ID = "pyflink_group"
TEST_SINK_TOPIC = "pyflink_sink"
PYTHON_EXECUTABLE = "C:/python_env/python38_flink/Scripts/python.exe"


def get_kafka_customer_properties(kafka_servers: str, group_id: str):
    """
    获取kafka 消费者 配置
    :return:
    """
    properties = {
        "bootstrap.servers": kafka_servers,
        "fetch.max.bytes": "67108864",
        "auto.offset.reset": "earliest",
        # "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        # "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "enable.auto.commit": "false",  # 关闭kafka 自动提交，此处不能传bool 类型会报错
        "group.id": group_id,
    }
    return properties


def get_kafka_producer_properties(servers):
    """
    kafka 生产者配置
    :param servers:
    :return:
    """
    properties = {
        "bootstrap.servers": servers,
        "max.request.size": "14999999"
    }
    return properties


def run():
    print(os.path.join(os.path.dirname(pyflink.__file__), 'lib'))
    print("================================================")
    # 获取运行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置环境
    # 配置检查点周期5分钟, 至少一次
    env.enable_checkpointing(60 * 1000 * 5, CheckpointingMode.AT_LEAST_ONCE)
    # 上一个检查点与下一个检查点之间必须间隔 3 分钟启动
    env.get_checkpoint_config().set_min_pause_between_checkpoints(60 * 1000 * 3)
    # 设置检查点超时40分钟
    env.get_checkpoint_config().set_checkpoint_timeout(60 * 1000 * 40)
    # 固定间隔重启策略 5分钟内若失败了3次则认为该job失败，重试间隔为30s
    env.set_restart_strategy(RestartStrategies.failure_rate_restart(3, timedelta(minutes=5), timedelta(seconds=30)))
    # 设置并行度
    env.set_parallelism(1)

    # 添加依赖的jar文件
    kafka_jar = "file:///D:/gitProject/pyFlink/lib/flink-connector-kafka_2.12-1.13.2.jar"
    kafka_client = "file:///D:/gitProject/pyFlink/lib/kafka-clients-2.8.0.jar"
    # env.add_jars(kafka_jar, kafka_client)

    # 添加文件
    # env.add_python_file(f"{os.getcwd()}/config_file.py")
    # env.add_python_file(f"{os.getcwd()}/env_setting.py")

    # 使用打包的运行环境 (自定义环境打包)
    # env.add_python_archive(f"{os.getcwd()}/venv.zip")
    # env.set_python_executable("env.zip/venv/bin/python")
    # 使用本地运行环境
    # env.set_python_executable(PYTHON_EXECUTABLE)
    # env.disable_operator_chaining()

    # 封装kafka配置信息
    kafka_product_properties = get_kafka_producer_properties(TEST_KAFKA_SERVERS)
    properties = get_kafka_customer_properties(TEST_KAFKA_SERVERS, TEST_GROUP_ID)

    data_stream = env.add_source(FlinkKafkaConsumer(topics=TEST_KAFKA_TOPIC,
                                                    properties=properties,
                                                    deserialization_schema=SimpleStringSchema()).set_commit_offsets_on_checkpoints(True)
                                 ).uid("test_kafka_source_000001").name(f"消费{TEST_KAFKA_TOPIC}主题数据")

    data_stream.print()
    # data_stream.map(lambda value: json.loads(s=value, encoding="utf-8")).name("转成json") \
    #     .map(lambda value: json.dumps(value), BasicTypeInfo.STRING_TYPE_INFO()).name("转成str") \
    #     .print()

    env.execute("测试pyflink读取kafka")


if __name__ == '__main__':
    run()
