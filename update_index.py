import asyncio
import hashlib
import itertools
from asyncio import get_event_loop
from concurrent.futures.process import ProcessPoolExecutor
from typing import Dict
from elasticsearch import Elasticsearch
import sys
import os

from crawl.youku import YoukuHandler

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

print(sys.path)
from crawl.tencent_video import TencentHandler
from settings import ES_HOST, INDEX_VIDEO_WEB

es_client = Elasticsearch(hosts=[ES_HOST, ], timeout=1000, http_auth=None)
spider_handlers = [TencentHandler(), YoukuHandler()]


def create_index(index_name: str = INDEX_VIDEO_WEB, index_settings: Dict = None):
    if index_settings is None:
        index_settings = {
            "settings": {
                "number_of_shards": 5,  # 配置主分片
                "number_of_replicas": 2,  # 配置副本节点
                # "analysis": {  # 配置分析器
                #     "char_filter": {  # 自定义字符过滤器
                #         "&_to_and": {
                #             "type": "mapping",
                #             "mappings": ["&=> and "]
                #         },
                #     },
                #     "filter": {  # 自定义词单元过滤器
                #         "my_stopwords": {
                #             "type": "stop",
                #             "stopwords": ["the", "a"]
                #         }
                #     },
                #     "analyzer": {  # 组合分析器
                #         "analyser1": {  # 自定义一个分析器
                #             "type": "custom",
                #             "char_filter": ["html_strip", "&_to_and"],
                #             "tokenizer": "standard",  # 标准分词器
                #             "filter": ["lowercase", "my_stopwords"]
                #         }
                #
                #     }
                # },
            },
            "mappings": {
                "dynamic": "false",  # 遇到新字段抛出异常(static)/忽略(false)/动态构造(true)
                "properties": {
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "channel": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "url": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }}}

    try:
        if not es_client.indices.exists(index_name):
            result = es_client.indices.create(index=index_name, ignore=400, body=index_settings)
            return result
    except Exception as e:
        raise e


def save_data(search_result: list):
    """
    将每次搜索的结果记录到es中，避免重复搜索
    :param search_result:
    :return:
    """

    for data in search_result:
        # 生成id
        title = data["title"]
        channel = data["channel"]
        url = data["url"]

        md5 = hashlib.md5()
        md5.update(title.encode())
        md5.update(channel.encode())
        md5.update(url.encode())

        dataId = md5.hexdigest()
        status = es_client.index(index=INDEX_VIDEO_WEB, body=data, id=dataId)
        print(status)


def search(query):
    loop = get_event_loop()
    tasks = []

    for handler in spider_handlers:
        tasks.append(handler.search(query=query))

    result_futures = loop.run_until_complete(asyncio.gather(*tasks))
    search_result_all = list(itertools.chain(*result_futures))
    print(search_result_all)
    return search_result_all


def start_crawler_multi(queries):
    # 抓取并保存结果
    create_index()

    def on_done(future):
        data = future.result()
        save_data(data)

    with ProcessPoolExecutor(max_workers=4) as executor:
        for query in queries:
            f = executor.submit(search, query)
            f.add_done_callback(on_done)


def read_test_data():
    result = set()
    with open('./crawl/video_name_data_test.txt', 'r', encoding='utf-8') as f:
        for row in f.readlines():
            result.add(row.strip())

    return list(result)


if __name__ == '__main__':
    video_name_list = read_test_data()
    log_file = open("./log.txt", "a", encoding="utf-8")

    for index in range(0, len(video_name_list), 100):
        queries = video_name_list[index:index + 100]
        print(index, queries, file=log_file)
        start_crawler_multi(queries=queries)
