# -*- coding: utf-8 -*-
import pandas as pd
import re
import io
from collections import defaultdict
import math
import operator
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def read_video_name():
    # 读原始短视频数据和video_name那一列
    sv = pd.read_csv('export_data1.csv')
    videoname = []
    # 用dict对aid去重
    videoname_dict = dict(zip(list(sv['aid']), list(sv['video_name'])))
    # 生成去重后的videoname合集
    for k, v in videoname_dict.items():
        videoname.append(str(v))
    return videoname


def cut_videoname(videoname):
    # 分词
    list_words = []
    every_query = {}
    for i in videoname:
        # 选用@作为划分的符号
        res0 = re.sub(r"[《》（）(),，·\[\]【】]", "@", i)
        # 对连续的、在query首位和末尾的@做处理
        res1 = re.sub(r"^@", "", res0)
        res2 = re.sub(r"@$", "", res1)
        res3 = re.sub(r"@{2,3}", "@", res2)
        words = res3.split("@")
        # 去掉空格占位的影响
        for z in words:
            if z == ' ':
                words.remove(z)
        # 生成原始query和分词之后的对应字典
        every_query[i] = words
        # 生成所有分词后的词典
        list_words.append(every_query[i])
    return list_words


def cal_idf(list_words):
    # 计算每个单词的idf
    # 文档总数
    doc_num = len(list_words)
    # 计算总词频
    doc_frequency = defaultdict(int)
    for word_list in list_words:
        for i in word_list:
            doc_frequency[i] += 1

    # 计算idf
    word_idf = {}
    for word in doc_frequency:
        word_idf[word] = math.log(doc_num / doc_frequency[word])
    return word_idf


def get_no_meaning_word(word_idf):
    # 生成"无用词词库"
    file_meaningless = io.open("no_meaning_words.csv", "w", encoding="UTF-8")
    sort_word_idf = sorted(word_idf.items(), key=operator.itemgetter(1), reverse=False)
    # 选取前0.3%的words进行人工筛选
    selected_no_meaning_words = [sort_word_idf[i] for i in range(int((0.004 * len(sort_word_idf))))]
    for j in selected_no_meaning_words:
        file_meaningless.write(list(tuple(j))[0] + '\n')
    return selected_no_meaning_words


def write_in_es():
    no_meaning_word = io.open("no_meaning_words_new.csv", "r", encoding="UTF-8")
    selected_no_meaning_words = no_meaning_word.readlines()
    selected_new_no_meaning_words = [i.strip('\n') for i in selected_no_meaning_words]
    words_dicts = {}
    for i in range(len(selected_new_no_meaning_words)):
        words_dicts[i] = selected_new_no_meaning_words[i]
    # print(words_dicts)
    es_client = Elasticsearch(hosts=[{"host": "192.168.10.82", "port": 8710}], timeout=1000, http_auth=None)
    assert es_client.ping(), "搜索服务连接失败！"

    """ 批量写入数据 """
    action = [{
        "_index": "tcl_shortvideo_no_meaning_words",
        # "_type": "doc",
        "_source": {
            "title": words_dicts[i]
        }
    } for i in range(len(words_dicts))]
    helpers.bulk(es_client, action)
	# 插入单条数据
	#es_client.index(index="tcl_shortvideo_no_meaning_words",body={"title":"花絮花絮" })


def delet_es_data():
    es_client = Elasticsearch(hosts=[{"host": "192.168.10.82", "port": 8710}], timeout=1000, http_auth=None)
    assert es_client.ping(), "搜索服务连接失败！"
    # 按照id单个删除
    # res = es_client.delete(index="tcl_shortvideo_no_meaning_words", id="Y0kLjnUBLTFohEJgdL8i")
    # 删除所有
    delete_by_all = {"query": {"match_all": {}}}
    result = es_client.delete_by_query(index="tcl_shortvideo_no_meaning_words", body=delete_by_all)


if __name__ == "__main__":
    """ 计算批量数据中的idf，得到无用词汇 """
    # videoname = read_video_name()
    # list_words = cut_videoname(videoname)
    # word_idf = cal_idf(list_words)
    """ 生成no_meaning_word,并进行人工检查"""
    # selected_no_meaning_words = get_no_meaning_word(word_idf)
    """人工校验no_meaning_word之后写入es"""
    write_in_es()
    #delet_es_data()
