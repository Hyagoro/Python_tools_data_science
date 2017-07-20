from elasticsearch import Elasticsearch

class Client:
    def __init__(self, host="localhost"):
        self.es = Elasticsearch(host=host)

    def query_elastic(self, index, doc_type, fields):
        def get(d, keys):
            for key in keys:
                d = d[key]
            return d

        query_all = \
            {"query": {
                "match_all": {}
            }
            }

        res = self.es.search(index=index,
                             doc_type=doc_type,
                             scroll='2m',
                             search_type='query_then_fetch',
                             size=1000,
                             body=query_all)

        print("Got %d Hits:\n" % res['hits']['total'])

        sid = res['_scroll_id']
        scroll_size = res['hits']['total']
        my_array = list()
                
        while scroll_size > 0:
            to_zip = []
            try:
                res = self.es.scroll(scroll_id=sid, scroll='2m')
                # Update the scroll ID
                sid = res['_scroll_id']
                scroll_size = len(res['hits']['hits'])
                for i in range(len(fields)):
                    list_json_tmp = []
                    list_json = res['hits']['hits']
                    for j in range(len(list_json)):
                        try:
                            json_raw = get(list_json[j], fields[i])
                        except:
                            json_raw = float('nan')
                        list_json_tmp.append(json_raw)
                        
                    to_zip.append(list_json_tmp)

                zippy = list(zip(*to_zip))
                
                my_array.extend(zippy)
            except Exception as e:
                print(e)
                break
        return my_array
