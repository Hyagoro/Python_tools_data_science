# Python tools for data science

### Elastic client

#### How to

    client = Client("localhost")
    result = client.query_elastic("index_name", "doc_type", [["_source", "field1"], ["_source", "field2"]])
    print(result)

    [(field1.1, field2.1), (field1.2, field2.2), ..., (field1.n, field2.n)]
