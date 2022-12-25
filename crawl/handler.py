import hashlib
import logging
import re
from typing import List, Dict
import aiohttp
from elasticsearch import Elasticsearch

from settings import ES_HOST, INDEX_VIDEO_WEB


class Request(object):
    pass


class BaseHandler(object):

    @staticmethod
    def remove_space(raw_text):
        """
        删除所有的空格和换行符
        :return: text without space
        """
        assert isinstance(raw_text, str)

        text_without_space = re.sub(r'[\n\t]*', "", raw_text)
        return text_without_space

    def get_url(self, query, channel="") -> str:
        """
        拼接返回对应的搜索链接
        :param query:
        :param channel:
        :return: url
        """
        pass

    async def fetch(self, url) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                html = await resp.text()
                return html

    def process(self, url, html) -> List:
        """
        :param url:
        :param html:
        :return:
        """
        raise NotImplementedError()

    async def search(self, query: str, channel: str = ''):
        """
        根据标题和视频类型，搜索视频网站，返回搜索结果页的第一页，并结构化
        :param query:
        :param channel: 分类
        :return:
        """
        search_result = []
        channel = '' if channel in ['all', ''] else channel
        url = self.get_url(query=query, channel=channel)

        try:
            html = await self.fetch(url)
            search_result = self.process(url, html)

        except Exception as e:
            logging.warning(f"请求网页异常,url: {url}, query: {query}, message: {str(e)}")
            raise e

        finally:
            return search_result




