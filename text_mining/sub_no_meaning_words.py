# -*- coding: utf-8 -*-
import pandas as pd
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def get_check_viedoname(videoname):
    # 连接es，读取无意义词汇表
    es_client = Elasticsearch(hosts=[{"host": "", "port": 8710}], timeout=1000, http_auth=None)
    assert es_client.ping(), "搜索服务连接失败！"
    res = es_client.search(index="tcl_shortvideo_no_meaning_words")
    response = res['hits']['hits']
    no_meaning_words = []
    for res in response:
        no_meaning_word = res['_source']['title']
        no_meaning_words.append(no_meaning_word)

    # 提取正片内容
    dict_check = {}
    dict_check["video_name"] = videoname
    ii = [i.strip('\n') for i in no_meaning_words]
    for iii in ii:
        videoname_new = re.sub(rf"{iii}","",videoname)
        res3 = re.sub(r"· ", "", videoname_new)
        res1 = re.sub(r"^·","",res3)
        res2 = re.sub(r"·$","",res1)
        res6 = re.sub(r"[,.，· ]$",'',res2)
        res4 = re.sub(r"（）", "", res6)
        res8 = re.sub(r"\(\)","",res4)
        res5 = re.findall(r"^《.*》$",res8)
        if res5:
            for i in res5:
                print(res5)
                res7 = re.sub(r"[《》]+","",i)
                dict_check['dealed_video_name'] = res7
                videoname = res7
        else:
            dict_check['dealed_video_name'] = res8
            videoname = res8

    return videoname

if __name__ == '__main__':
    # no_meaning_word = get_no_meaning_word()
    # video_name = get_video_name()
    # 需要再次校验的正片内容
    video_name = '[喜宝]，预告花絮速看CUT'
    check_videoname = get_check_viedoname(video_name)
    #{"video_name":"y","meaning":}
    #print(check_videoname)
