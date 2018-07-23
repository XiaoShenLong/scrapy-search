

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from baike.models.es_types import BaiduType,gen_suggest
from scrapy.loader.processors import MapCompose
import redis
from baike import settings

redis_cli = redis.StrictRedis(host=settings.REDIS_HOST)

def tianjia(value):
    return value


class BaikeItem(scrapy.Item):
    # 标题
    title = scrapy.Field(
        input_processor=MapCompose(tianjia),
    )
    # url
    url = scrapy.Field()
    # 概述
    summary = scrapy.Field(
        input_processor=MapCompose(tianjia),
    )
    # 详细
    content = scrapy.Field(
        input_processor=MapCompose(tianjia),
    )

    def save_to_es(self):
        baike = BaiduType()
        baike.title = self['title']
        baike.url = self['url']
        baike.summary = self['summary']
        #baike.basicinfo = self['basicinfo']
        baike.content = self['content']

        baike.suggest = gen_suggest(BaiduType._doc_type.index,((baike.title,10),(baike.summary,7)))


        baike.save()
        redis_cli.incr("baike_count")

        return

