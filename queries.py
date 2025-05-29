# queries.py
import argparse
import json
from elasticsearch import Elasticsearch
import local_settings as settings

def pretty_print(response):
    data = response.body if hasattr(response, "body") else response
    print(json.dumps(data, indent=2, ensure_ascii=False))

def search_and_show(es, index, body):
    resp = es.search(index=index, body=body)
    hits = resp["hits"]["hits"]
    if not hits:
        print("No documents matched your query.")
    else:
        print(json.dumps(hits, indent=2, ensure_ascii=False))

def main():
    p = argparse.ArgumentParser(description="Run ES queries")
    p.add_argument(
        "-i", "--index",
        default=settings.DEFAULT_INDEX,
        help="Elasticsearch index to query"
    )
    p.add_argument("--host",    default=settings.ES_HOST)
    p.add_argument("--user",    default=settings.ES_USER)
    p.add_argument("-p", "--password", default=settings.ES_PASSWORD)
    args = p.parse_args()

    es = Elasticsearch(
        args.host,
        basic_auth=(args.user, args.password),
        verify_certs=True
    )


    search_and_show(es, args.index, {
        "query": {"term": {"entity_id": {"value": 42}}}
    })

if __name__ == "__main__":
    main()
