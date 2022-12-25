import re
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

from elasticsearch import Elasticsearch
from settings import ES_HOST, INDEX_VIDEO, INDEX_VIDEO_WEB, INDEX_NO_MEANING_WORDS


class Video(object):
    def __init__(self, video_name, channel, aid):
        self.video_name = video_name
        self.channel = channel
        self.aid = aid


class Processor(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        """ 单例模式 """
        if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kwargs)

        return cls._instance

    def __init__(self):
        # todo 增加日志管理
        self.es_client = Elasticsearch(hosts=[ES_HOST], timeout=1000, http_auth=None)
        assert self.es_client.ping(), f"搜索服务连接失败{ES_HOST}"

        self.no_meaning_words_index = INDEX_NO_MEANING_WORDS  # 无意义词库索引
        self.crawler_data_index = INDEX_VIDEO_WEB  # 爬虫影片名索引
        self.tcl_album_index = INDEX_VIDEO  # 媒资库专辑索引

    def search_database(self, query: str, index: str, text: str, channel: str) -> bool:
        """
        搜索媒资库，返回搜索结果
        :param text: 待校验的标题或文本
        :param channel: 待校验的channel
        :param index: 搜索的索引名称
        :param query: str
        :return:
        """
        titleConfirmed = False

        resp = self.es_client.search(body=query, index=index)

        if not resp["hits"]["total"]["value"]:
            titleConfirmed = False

        for item in resp["hits"]["hits"]:
            source = item["_source"]
            if source["title"] == text and source["channel"] == channel:
                titleConfirmed = True
                break

        return titleConfirmed

    def is_title(self, text, channel) -> bool:
        """
        通过爬虫&已有的媒资库数据验证video_name字段是否为正片标题
        :param channel:
        :param text: str
        :return: True/False
        """

        confirmed = False
        # 搜索es数据库，是否存在完全同名正片
        query_index = {self.tcl_album_index: {
            "query": {
                "bool": {
                    "must": [{"term": {"title.keyword": {
                        "value": text,
                        "boost": 1.0
                    }}},
                        {"term": {"type": {
                            "value": 1,
                            "boost": 1.0
                        }}},
                        {"term": {"channel.keyword": {
                            "value": channel,
                            "boost": 1.0
                        }}

                        }]}}
        }, self.crawler_data_index: {"query": {
            "bool": {
                "must": [{"match": {"title": text}},
                         {"match": {"channel": channel}}
                         ]}}
        }}

        for index in [self.tcl_album_index, self.crawler_data_index]:
            searched_in_database = self.search_database(query=query_index[index], text=text, index=index,
                                                        channel=channel)

            if searched_in_database:
                confirmed = True
                break

        return confirmed

    def text_mining(self, text) -> dict:
        """
        尝试从video_name中提取正片名，区分无意义词
        :param text:
        :return:
        """
        query = {"query": {"match_all": {}}}
        res = self.es_client.search(index=self.no_meaning_words_index, body=query, size=10000)
        response = res['hits']['hits']
        no_meaning_words = []

        for res in response:
            no_meaning_word = res['_source']['title']
            no_meaning_words.append(no_meaning_word)

        ii = [i.strip('\n') for i in no_meaning_words]
        for iii in ii:
            videoname_new = re.sub(rf"{iii}", "", text)
            # res3 = re.sub(r"· ", "", videoname_new)
            res1 = re.sub(r"^·", "", videoname_new)
            res2 = re.sub(r"·$", "", res1)
            res6 = re.sub(r"[,，· ]$", '', res2)
            res4 = re.sub(r"（）", "", res6)
            res8 = re.sub(r"\(\)", "", res4)
            res9 = re.sub(r"^:", "", res8)
            res10 = re.sub(r"^：", "", res9)
            res5 = re.findall(r"^《.*》$", res10)
            if res5:
                for i in res5:
                    res7 = re.sub(r"[《》]+", "", i)
                    text = res7
            else:
                text = res10
        return text

    def parse(self, video: Video):
        """
        校验短视频信息，目前只校验video_name字段的准确性和合理性
        :param video:
        :return:
        """
        status = defaultdict(lambda: None)

        # 只校验video_name字段的准确性
        video_name = video.video_name
        channel = video.channel
        # 从video_name中提取正片标题，区分无意义词汇
        meaningful_text = self.text_mining(video_name)
        status["video_name_formatted"] = meaningful_text

        if not meaningful_text:
            # 没有提取到有意义的标题，video_name判定为不合格
            status['video_name_status'] = False
            return status

        # 校验提取到的词汇是否为正片标题
        is_title = self.is_title(text=meaningful_text, channel=channel)
        status["video_name_status"] = True if is_title else False

        return dict(status)

    def on_done(self, video: Video, parse_result):
        """
        todo 更新数据库中对应的短视频结果
        :param video:
        :param parse_result:
        :return:
        """
        print(video.video_name, parse_result)

    def parse_multi(self, videos: list, max_workers=4):
        """
        多进程 + 协程处理数据
        :param max_workers:
        :param videos:
        :return:
        """

        def on_done(future, video: Video):
            # 每条数据处理完成后逻辑
            parse_result = future.result()

            # 更新数据库对应短视频数据
            self.on_done(video, parse_result)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for video in videos:
                future = executor.submit(self.parse, video)
                future.add_done_callback(partial(on_done, video=video))


if __name__ == '__main__':
    video = Video(video_name='竖屏剧场达人解读', channel='电影', aid='')
    video1 = Video(video_name='宝贝', channel='电影', aid='')

    processor = Processor()
    # processor.checking(video1)
    processor.parse_multi([video, video1])
