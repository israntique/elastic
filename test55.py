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

# Corrected mapping structure
mapping = {
    "analysis": {
      "tokenizer": {
        "my_ngram_tokenizer": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 20,
          "token_chars": ["letter", "digit"]
        }
      },
      "analyzer": {
        "my_ngram_analyzer": {
          "type": "custom",
          "tokenizer": "my_ngram_tokenizer"
        },
        "my_search_analyzer": {
          "type": "standard"
        }
      }
    },
  
    
    "mappings": {  # Add the 'mappings' key here
        "properties": {
            "entity_id": { "type": "integer" },
            "origin_id": { "type": "keyword" },
            "origin_table": { "type": "keyword" },
            "thes": {
                "type": "nested",
                "properties": {
                    "wid": { "type": "integer" },
                    "heb": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
                    "eng": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
                    "cod_thes": { "type": "integer" }
                }
            },
            "txt": {
                "type": "nested",
                "properties": {
                    "type": { "type": "keyword" },
                    "entity_text_string": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
                    "subtype": { "type": "keyword" }
                }
            },
            "RISH": {
                "type": "nested",
                "properties": {
                    "entity_id": { "type": "integer" },
                    "license_id": { "type": "integer" },
                    "license_long_name": { "type": "keyword" },
                    "license_short_name": { "type": "keyword" },
                    "shape_id": { "type": "integer" },
                    "shape": { "type": "geo_shape" },
                    "shape_wkt": { "type": "keyword" }
                }
            }
        }
    }
}



# Delete old index if exists
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Old index '{index_name}' deleted.")


# Check if the index already exists
if not es.indices.exists(index=index_name):
    # Create the index with the mapping
    try:
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created successfully with mapping.")
    except Exception as e:
        print(f"Error creating index '{index_name}': {e}")
else:
    print(f"Index '{index_name}' already exists.")
    # You might want to update the mapping if it's different from what you expect
    #  es.indices.put_mapping(index=index_name, body=mapping['mappings']) #update the mapping
    #  print(f"Index '{index_name}' already exists.  Mapping not updated.")
    pass


# Load JSON data from file
with open("data.json", "r", encoding="utf-8") as file:
    data = json.load(file)
    print('after load data')
# Index the documents
for doc in data:
    es.index(index=index_name, id=doc["entity_id"], body=doc)

print("Documents indexed successfully!")

query={
  "query": {
    "bool": {
      "must": [
          {
          "nested": {
            "path": "txt",
            "query": {
              "bool": {
                "must": [
                  {
                    "match": {
                      "txt.entity_text_string": "מטבע"
                    }
                  }
                ]
              }
            }
          }
        },
         {
           "nested": {
             "path": "RISH",
             "query": {
               "bool": {
                 "must": [
                   {
                     "term": {
                       "RISH.license_long_name": "G-11/2018"
                     }
                   }
                 ]
               }
             }
           }
         }
      ]
    }
  }
}

# Execute search query
response = es.search(index=index_name, body=query)
# Print entity_ids of matching documents
print("\nEntity IDs containing 'RISH':")
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"])
#######################################################################################################################
## Qeuring with OR oparation    using SHARE
queryOR={
  "query": {
    "bool": {
      "should": [
        {
          "bool": {
            "must": [
              {
                "nested": {
                  "path": "txt",
                  "query": {
                    "bool": {
                      "must": [
                        {
                          "match": {
                            "txt.entity_text_string": {
                              "query": "מטבע",
                              "operator": "OR"
                            }
                          }
                        }
                      ]
                    }
                  }
                }
              }
            ],
            "filter": [
              {
                "exists": {
                  "field": "txt.entity_text_string"
                }
              }
            ]
          }
        },
        {
          "bool": {
            "must": [
              {
                "nested": {
                  "path": "RISH",
                  "query": {
                    "bool": {
                      "must": [
                        {
                          "term": {
                            "RISH.license_long_name": {
                              "value": "G-11/2018",
                              "operator": "OR"
                            }
                          }
                        }
                      ]
                    }
                  }
                }
              }
            ],
            "filter": [
              {
                "exists": {
                  "field": "RISH.license_long_name"
                }
              }
            ]
          }
        }
      ]
    }
  }
}

# Execute search query
response = es.search(index=index_name, body=queryOR)
# Print entity_ids of matching documents
#print("\nEntity IDs containing 'QUERY OR':")
#for hit in response["hits"]["hits"]:
 #   print(hit["_source"]["entity_id"])