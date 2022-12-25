import asyncio
import logging
from urllib.parse import urljoin, urlencode
from lxml import etree

from crawl.handler import BaseHandler


class TencentHandler(BaseHandler):

    def get_url(self, query, channel="all"):
        """
        腾讯搜索链接工厂函数
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

        channel_index = mapping_channel.get(channel, 0)
        #base_url = 'https://v.qq.com/x/search'
        base_url = ''
        query_params = {
            "q": query,
            "needCorrect": query,
            "stag": 4,
            "filter": f"sort=0&pubfilter=0&duration=0&tabid={channel_index}&resolution=0"
        }
        url = base_url + "?" + urlencode(query_params)
        return url

    def process(self, url, html):
        """
        :param url:
        :param html:
        :return:
        """
        search_result = []
        et = etree.HTML(html)
        resultTags = et.xpath('//h2[@class="result_title"]/a')
        if not resultTags:
            return search_result

        for aTag in resultTags:
            full_text = ''.join(aTag.xpath('.//text()'))
            span_text = ''.join(aTag.xpath('.//span//text()'))
            channel_text = ''.join(aTag.xpath('.//span[@class="type"]//text()'))
            channel = channel_text if channel_text else '其他'
            href = aTag.xpath('./@href')[0]
            url = urljoin(url, href)
            result_title = self.remove_space(full_text).replace(self.remove_space(span_text), "")
            if result_title and url:
                search_result.append({'title': result_title, 'channel': channel, 'url': url})
        logging.debug(search_result)
        logging.debug(search_result)
        return search_result


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    handler = TencentHandler()
    loop.run_until_complete(handler.search('猫小帅魔法城堡'))
