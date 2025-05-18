import json
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "12345678"
es = Elasticsearch("http://localhost:9200",
                   basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),  # Authentication credentials
                   verify_certs=True)
#print(es.info())


# Define the index name
index_name = "entities"
# Delete index if it exists
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# Define the mapping
mapping = {
    "settings": {
        "analysis": {
            "normalizer": {
                "lowercase_normalizer": {
                    "type": "custom",
                    "char_filter": [],
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "entity_id": {"type": "integer"},
            "origin_id": {"type": "keyword"},
            "origin_table": {"type": "keyword"},
            
            # Text Data (supports both full-text and exact match)
            "txt": {
                "type": "nested",
                "properties": {
                    "type": {"type": "keyword"},
                    "subtype": {"type": "keyword"},
                    "entity_text_string": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    }
                }
            },

            # Shape data for Geo-Spatial queries
            "shp": {
                "type": "nested",
                "properties": {
                    "shape_id": {"type": "integer"},
                    "oshape_id": {"type": "integer"},
                    "shape_wkt": {"type": "text"},
                    "shape": {"type": "geo_shape"}  # Geo Shape
                }
            },

            # License and Permit Data
            "RISH": {
                "type": "nested",
                "properties": {
                    "entity_id": {"type": "integer"},
                    "license_id": {"type": "integer"},
                    "license_long_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "license_short_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "shape_id": {"type": "integer"},
                    "shape_wkt": {"type": "text"},
                    "shape": {"type": "geo_shape"}
                }
            },

            # Thesaurus (for objects like coins)
            "thes": {
                "type": "nested",
                "properties": {
                    "wid": {"type": "integer"},
                    "heb": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "eng": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "cod_thes": {"type": "integer"}
                }
            }
        }
    }
}

# Create index (if it doesn't exist)
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully!")


    # Load JSON data from file
with open("data.json", "r", encoding="utf-8") as file:
    data = json.load(file)
    print('after load data')
# Index the documents
for doc in data:
    es.index(index=index_name, id=doc["entity_id"], body=doc)

print("Documents indexed successfully!")

query_1 = {
    "query": {
        "bool": {
            "must": [
                {"nested": {
                    "path": "RISH",
                    "query": {"match": {"RISH.license_long_name.keywords": "A-3038/1999"}}
                }},
                {"nested": {
                    "path": "txt",
                    "query": {"match": {"txt.entity_text_string.keywords": "ירדנה"}}
                }}
            ]
        }
    }
}

query_2 = {
  "query": {
    "bool": {
      "must": [
        {
          "nested": {
            "path": "RISH",
            "query": {
              "match": {
                "RISH.license_long_name.keyword": "A-3038/1999"
              }
            }
          }
        },
        {
          "nested": {
            "path": "txt",
            "query": {
              "match": {
                "txt.entity_text_string": "ירדנה"
              }
            }
          }
        }
      ]
    }
  }
}

query={
  "query": {
    "match_all": {}
  }
}
response = es.search(index=index_name, body=query)
for hit in response["hits"]["hits"]:
    print(hit["_source"])