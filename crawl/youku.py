import asyncio
import json
import logging
import re
from urllib.parse import urljoin, quote, urlencode

import aiohttp
import jsonpath
from fake_useragent import UserAgent

from crawl.handler import BaseHandler


class YoukuHandler(BaseHandler):

    def get_url(self, query, channel=""):
        """
        爱奇艺搜索链接工厂函数
        :param query:
        :param channel:
        :return:
        """
        channel = 'all' if channel in ['', 'all'] else channel
        mapping_channel = {
            "all": 0,
            "tv": 2,
            "movie": 1,
            "variety": 3,
            "child": 106,
            "cartoon": 4,
            "doco": 6
        }

        # channel_index = mapping_channel.get(channel, 0)
        #base_url = "https://search.youku.com/api/search"
        base_url = ""
        querystring = {"keyword": query,
                       "userAgent": "Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML,+like+Gecko%29+Chrome%2F84.0.4147.105+Safari%2F537.36",
                       "site": "1", "aaid": "135d25e581d121ce1bdecbd98d7d45b5", "duration": "0-0", "categories": "0",
                       "ftype": "0", "ob": "0", "cna": "a76yF%2F79c0UCAXBhPG7VpgLJ", "pg": "1"}

        # url = base_url + "?" + urlencode(querystring)
        url = ""
        # url = f'https://search.youku.com/api/search?keyword={quote(query)}&pg=1'
        return url

    async def fetch(self, url) -> str:

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                return data

    def process(self, baseUrl, data):
        """
        :param baseUrl:
        :param data:
        :return:
        """
        search_result = []
        contents = data['originNodes']

        if not contents:
            return search_result

        for item in contents:
            for data in jsonpath.jsonpath(item, '$..data'):
                # [?( )] 用于从列表中选择子节点
                channelList = jsonpath.jsonpath(data, '$.cats')
                channel = channelList[0] if channelList else '其他'

                # 找到同时包含title和url 或者 同时包含title 和sourceList的节点
                titleList = jsonpath.jsonpath(data, '$.titleDTO.displayName')
                scmList = jsonpath.jsonpath(data, '$.titleDTO.action.report.scm')
                # valueList = jsonpath.jsonpath(data, '$.action.value')
                valueList = []
                title = url = None
                if titleList and len(titleList) == 1:
                    title = titleList[0]

                if valueList and len(valueList) == 1:
                    url = valueList[0]

                else:
                    if scmList and len(scmList) == 1:

                        scm = scmList[0]
                        video_id = re.search(r'_(?P<id>.*)$', scm).group('id')
                        url = f'https://m.youku.com/alipay_video/id_{video_id}.html'

                if title and url:
                    title = title.replace(r'&nbsp;&nbsp;', "  ")
                    url = urljoin(baseUrl, url)
                    logging.debug(title, url, channel)
                    search_result.append({'title': title, 'channel': channel, 'url': url})
        logging.debug(search_result)
        logging.debug(search_result)
        return search_result


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    handler = YoukuHandler()
    loop.run_until_complete(handler.search('猫小帅魔法城堡'))
