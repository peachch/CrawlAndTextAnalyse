
import asyncio
import logging
from urllib.parse import urljoin, quote

from lxml import etree

from crawl.handler import BaseHandler


class QiyiHandler(BaseHandler):

    def get_url(self, query, channel=""):
        """
        爱奇艺搜索链接工厂函数
        :param query:
        :param channel:
        :return:
        """
        query = quote(query)
        channel = quote(channel)
        url = ''
        #url = f'https://so.iqiyi.com/so/q_{query}_ctg_{channel}_t_0_page_1_p_1_qc_0_rd__site__m_1_bitrate__af_1'
        return url

    def process(self, url, html):
        """
        :param url:
        :param html:
        :return:
        """
        search_result = []
        et = etree.HTML(html)
        resultTags = et.xpath('//*[@class="result-right"]/h3')
        if not resultTags:
            return search_result

        for tag in resultTags:
            title = tag.xpath('./a/@title')[0]
            channel_tag = tag.xpath('./span[@class="item-type"]/text()')
            channel = channel_tag[0] if channel_tag else '其他'
            href = tag.xpath('./a/@href')[0]
            url = urljoin(url, href)

            if title and url:
                search_result.append({'title': title, 'channel': channel, 'url': url})
        logging.debug(search_result)
        return search_result


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    handler = QiyiHandler()
    loop.run_until_complete(handler.search('猫小帅魔法城堡'))
