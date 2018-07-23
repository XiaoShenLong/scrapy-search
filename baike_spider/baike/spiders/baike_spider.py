import sys
sys.getdefaultencoding()

from urllib.parse import unquote

import scrapy
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
import re
import logging

from scrapy_redis.spiders import RedisCrawlSpider

from baike.items import BaikeItem
from baike.mysql_conn import Mysql


class MySpider(RedisCrawlSpider):
    name = 'baike'
    allowed_domains = ['baike.baidu.com']

    redis_key = 'baike:start_urls'


    rules = (
        Rule(LinkExtractor(allow=('baike.baidu.com', ), deny=('https?://baike.baidu.com/item','https?://baike.baidu.com/subview',
                                               'https?://baike.baidu.com/view','https://baike.baidu.com/tashuo','https?://baike.baidu.com/pic',
                                               'https://baike.baidu.com/history','https://baike.baidu.com/historypic',
                                               'http://baike.baidu.com/mall','https?://baike.baidu.com/albums?',
                                                'https?://baike.baidu.com/article','https://baike.baidu.com/difangzhi'
                                               'http://baike.baidu.com/campus','https://baike.baidu.com/redirect',
                                                'https://baike.baidu.com/divideload'
                                               )),process_links='process_links',follow=True),

        # Extract links matching 'item.php' and parse them with the spider's method parse_item
        Rule(LinkExtractor(allow=('https?://baike.baidu.com/subview',)), callback='parse_word',follow=True,process_links="proc_subview_links"),
        Rule(LinkExtractor(allow=('https?://baike.baidu.com/view',)), callback='parse_word',follow=True,process_links="proc_view_links"),
        Rule(LinkExtractor(allow=('https?://baike.baidu.com/item', )), callback='parse_word',follow=True,process_links="proc_item_links"),
    )

    db = Mysql()
    #过滤掉已经访问过的请求
    def process_links(self,links):
        for link in links:
            try:
                self.db.insert_urlfilter(unquote(link.url))
            except:
                links.remove(link)
        return links

    def proc_item_links(self,links):
        for link in links:
            try:
                self.db.insert_itemfilter(re.split('[#]',unquote(link.url[link.url.find('/item/')+6:]))[0])
            except:
                links.remove(link)
        return links

    def proc_view_links(self,links):
        for link in links:
            try:
                self.db.insert_viewfilter(unquote(link.url))
            except:
                links.remove(link)
        return links

    def proc_subview_links(self,links):
        for link in links:
            try:
                self.db.insert_subviewfilter(unquote(link.url))
            except:
                links.remove(link)
        return links

    def parse_word(self,response):
        title = response.xpath("//dl[contains(@class,'lemmaWgt-lemmaTitle')]//h1/text()").extract()
        summary=response.xpath("//div[@class='lemma-summary']")
        polysemy = response.xpath("//div[contains(@class,'polysemant-list')]//li[@class='item']/span[@class='selected']/text()")
        unquote_url=unquote(response.url)
        chapter = response.xpath("//div[@class='para-title level-2']")
        item=BaikeItem()
        #判断该页面是否为词条页面 通过是否存在title和摘要判断  url例外https://baike.baidu.com/item/%E7%9B%9B%E5%AE%A3%E6%80%80?force=1
        if len(title)!=0 and len(summary)!=0:
            #判断是否存在同义词条
            if response.url.find('fromtitle')!=-1 and '同义词' in response.xpath("//span[@class='view-tip-panel']/a/text()").extract():
                syn_word=self.process_synonym_url(unquote_url)
                syn_from= syn_word[0].split('/item/')
                syn=syn_word[0]
                if len(syn_from) == 2:
                    syn=syn_from[1]
                else:
                    logging.log("同义词url出现问题："+unquote_url)

                if len(syn_word)==3:
                    #原始词的url
                    pre_url='https://baike.baidu.com/item/' + syn_word[1] + '/' + syn_word[2]
                    if syn_word[2]=='':
                        syn_word[2]=0
                    try:
                        if len(polysemy.extract()) == 0:
                            self.db.insert_wordinfo(pre_url,syn_word[1],int(syn_word[2]),synonym=syn)
                        else:
                            self.db.insert_wordinfo(pre_url,syn_word[1],int(syn_word[2]),polysemy.extract()[0],synonym=syn)
                    except Exception as e:
                        logging.error("错误："+str(e))
                        logging.warning("该同义词已添加到数据库："+unquote_url)
                    yield scrapy.Request(syn_word[0])
            else:
                #将该词条保存
                wordid=self.process_word_url(unquote_url)
                try:
                    if len(polysemy.extract())==0:
                        self.db.insert_wordinfo(unquote_url,title[0],wordid)
                    else:
                        self.db.insert_wordinfo(unquote_url,title[0],wordid,polysemy.extract()[0])
                        # 将该次的信息写入文件中

                    item['title'] = title[0]
                    item['url'] = unquote_url
                    item['summary'] = "".join(summary.xpath('.//text()').extract())
                    # 词的内容提取，只包含二级标题，忽略三级标题
                    content = ''
                    content_a = []
                    content_href = []
                    for ind, ch in enumerate(chapter, start=1):
                        content_para = ch.xpath("./following-sibling::div[@class='para'][count(preceding-sibling::"
                                                "div[@class='para-title level-2'])=%d]" % ind)
                        for con in content_para:
                            content += "".join((con.xpath('.//text()').extract())).strip()
                            content_a.extend(con.xpath(
                                ".//a[starts-with(@href,'/item')]//text()").extract())
                            content_href.extend([unquote(x) for x in con.xpath(
                                ".//a[starts-with(@href,'/item')]/@href").extract()])
                    item['content'] = content
                    yield item
                except Exception as e:
                    logging.error("错误：" + str(e))
                    logging.warning("该词已添加到数据库：" + unquote_url)
        else:
            #对可能存在的意外进行日志记录
            logging.warning("非词条页，请确认： "+unquote_url)
# 普通的url链接处理
    def process_word_url(self,url):
        wordid=re.findall('/\d*$', url.split('?')[0])
        if len(wordid)==0:
            wordid=0
        else:
            wordid=int(wordid[0][1:])
        return wordid
# 同义词处理
    def process_synonym_url(self,url):
        if url.rfind('?') != -1:
            url_list = url.split('?')
            if 'fromtitle=' in url_list[1] and 'fromid=' in url_list[1]:
                wl=url_list[1].split('&')
                
                for w in wl:
                    if 'fromtitle=' in w:
                        title=w[w.find('fromtitle=') + 10:len(w)]
                    if 'fromid=' in w:
                        id_s = w.rfind('fromid=') + 7
                        id_e = w.rfind('#')
                        if id_e == -1:
                            id_e = len(w)
                        id=w[id_s:id_e]
                        
                return (url_list[0],title, id)
            else:
                return -1
        return -1



