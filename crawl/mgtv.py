
import asyncio
import logging
from urllib.parse import urljoin, quote

import aiohttp
import jsonpath

from crawl.handler import BaseHandler


class MgtvHandler(BaseHandler):

    def get_url(self, query, channel=""):
        """
        爱奇艺搜索链接工厂函数
        :param query:
        :param channel:
        :return:
        """
        channel = 'all' if channel in ['', 'all'] else channel
        mapping_channel = {
            "all": -99,
            "tv": 2,
            "movie": 3,
            "variety": 1,
            # "child": 106,
            "cartoon": 50,
            "doco": 51,
        }

        channel_index = mapping_channel.get(channel, -99)
        query = quote(query)
        url = ""
        # url = f'https://mobileso.bz.mgtv.com/pc/search/v1?q={query}&pn=1&pc=10&uid=&ty={channel_index}'
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
        contents = data['data']['contents']

        if not contents:
            return search_result

        for item in contents:
            # [?( )] 用于从列表中选择子节点
            channelList = jsonpath.jsonpath(item, '$..desc[?(@.label=="类型")].text')
            channel = channelList[0] if channelList else '其他'

            for data in jsonpath.jsonpath(item, '$..*'):
                # 找到同时包含title和url 或者 同时包含title 和sourceList的节点
                titleList = jsonpath.jsonpath(data, '$.title')
                urlList = jsonpath.jsonpath(data, '$.url')
                sourceList = jsonpath.jsonpath(data, '$.sourceList[*].url')

                title = url = None
                if titleList and len(titleList) == 1:
                    title = titleList[0]

                if urlList and len(urlList) == 1:
                    url = urlList[0]

                else:
                    if sourceList and len(sourceList) >= 1:
                        url = sourceList[0]

                if title and url:
                    title = title.replace(r'&nbsp;&nbsp;', "  ")
                    url = urljoin(baseUrl, url)
                    search_result.append({'title': title, 'channel': channel, 'url': url})
        logging.debug(search_result)
        logging.debug(search_result)
        return search_result


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    handler = MgtvHandler()
    loop.run_until_complete(handler.search('猫小帅魔法城堡'))