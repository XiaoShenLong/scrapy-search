from django.shortcuts import render
import json
from django.views.generic.base import View
from search.es_type import BaiduType
from django.http import HttpResponse
from elasticsearch import Elasticsearch
from datetime import datetime
import redis
from BaikeSearch import settings
import pickle
client = Elasticsearch(hosts=[settings.ES_HOST])

redis_cli = redis.StrictRedis(host=settings.REDIS_HOST)
# Create your views here.


class IndexView(View):
    def get(self,request):
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5)
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    def get(self,request):
        key_words = request.GET.get('s','')
        re_datas = []
        if key_words:
            s = BaiduType.search()
            s = s.suggest("my_suggest",key_words,completion = {
                "field":"suggest",
                "fuzzy":{
                    "fuzziness":2
                },
                "size":10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                source = match._source
                re_datas.append(source["title"])
        return HttpResponse(json.dumps(re_datas),content_type="application/json")

class SearchView(View):
    def get(self,request):
        key_words = request.GET.get('q','')
        redis_cli.zincrby("search_keywords_set",key_words)
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5)
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean

        page = request.GET.get("p","1")
        try:
            page = int(page)
        except:
            page = 1

        baike_count = str(redis_cli.get("baike_count"),encoding="utf-8")

        start_time = datetime.now()
        response = client.search(
            index="baidu",
            body={
                "query":{
                    "multi_match":{
                        "query":key_words,
                        "fields":["title","summary"]
                    }
                },
                "from":(page-1)*10,
                "size":10,
                "highlight" : {
                    "pre_tags": ['<span class="keyWord">'],
                    "post_tags": ['</span>'],
                    "fields" : {
                        "title" : {},
                        "summary": {},

                    }
                 }
            }
        )
        end_time = datetime.now()
        last_seconds = (end_time - start_time).total_seconds()

        total_nums = response["hits"]["total"]
        if (page%10) > 0:
            page_nums = int(total_nums/10 + 1)
        else:
            page_nums = int(total_nums/10)
        hit_list = []
        for hit in response["hits"]["hits"]:
            hit_dict = {}
            if "title" in hit["highlight"]:
                hit_dict["title"] = "".join(hit["highlight"]["title"])
            else:
                hit_dict["title"] = hit["_source"]["title"]
            if "summary" in hit["highlight"]:
                hit_dict["summary"] = "".join(hit["highlight"]["summary"][:500])
            else:
                hit_dict["summary"] = hit["_source"]["summary"][:500]

            hit_dict["content"] = hit["_source"]["content"]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]
            hit_list.append(hit_dict)

        return render(request , "result.html",{
                                                "page":page,
                                                "total_nums":total_nums,
                                                "all_hits":hit_list,
                                                "key_words":key_words,
                                                "page_nums":page_nums,
                                                "last_seconds":last_seconds,
                                                "baike_count":baike_count,
                                                "topn_search":topn_search,
                                            })


