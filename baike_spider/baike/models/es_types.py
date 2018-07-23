from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerObjectWrapper, Completion, Keyword, Text

from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer
from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=["140.143.211.106"])



class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}

ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])


class BaiduType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    title = Text(analyzer="ik_max_word")
    summary = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")

    class Meta:
        index = "baidu"
        doc_type = "baike"



def gen_suggest(index, info_tuple):
    # 根据字符串生成搜索建议数组
    es = connections.create_connection(BaiduType._doc_type.using,hosts=["140.143.211.106"])     # 连接elasticsearch(搜索引擎)，使用操作搜索引擎的类下面的_doc_type.using连接
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串，
            words = es.indices.analyze(index="baidu", analyzer="ik_max_word", params={'filter': ["lowercase"]}, body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"])>1])
            new_words = anylyzed_words - used_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input":list(new_words), "weight":weight})
    return suggests


if __name__ ==  "__main__":
    BaiduType.init()