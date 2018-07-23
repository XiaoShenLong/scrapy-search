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
    #basicinfo = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")

    class Meta:
        index = "baidu"
        doc_type = "baike"



def gen_suggest(index, info_tuple):
    # 根据字符串生成搜索建议数组
    """
    此函数主要用于,连接elasticsearch(搜索引擎)，使用ik_max_word分词器，将传入的字符串进行分词，返回分词后的结果
    此函数需要两个参数：
    第一个参数：要调用elasticsearch(搜索引擎)分词的索引index，一般是（索引操作类._doc_type.index）
    第二个参数：是一个元组，元祖的元素也是元组，元素元祖里有两个值一个是要分词的字符串，第二个是分词的权重，多个分词传多个元祖如下
    书写格式：
    gen_suggest(lagouType._doc_type.index, (('字符串', 10),('字符串', 8)))
    """
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