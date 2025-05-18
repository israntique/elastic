import json
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "12345678"
es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
    verify_certs=True
)

info = es.info()
print(info['version']['number'])  # will print something like '8.17.3'
# index_name = "entities99"

# # Delete index if already exists
# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)

# # Create the index with mapping
# mapping1={
#   "settings": {
#     "analysis": {
#       "tokenizer": {
#         "my_ngram_tokenizer": {
#           "type": "edge_ngram",
#           "min_gram": 2,
#           "max_gram": 20,
#           "token_chars": ["letter", "digit"]
#         }
#       },
#       "analyzer": {
#         "my_ngram_analyzer": {
#           "type": "custom",
#           "tokenizer": "my_ngram_tokenizer"
#         },
#         "my_search_analyzer": {
#           "type": "standard"
#         }
#       }
#     }
#   },
#   "mappings": {
#     "properties": {
#        "txt": {
#             "type": "nested",
#             "properties": {
#                 "type": {"type": "keyword"},
#                 "subtype": {"type": "keyword"},
#                 "entity_text_string": {
#                     "type": "text",
#                     "analyzer": "hebrew", # <--- APPLY HEBREW ANALYZER HERE
#                     "fields": {
#                         "ngram": { # Keep your ngram field for partial search if needed
#                             "type": "text",
#                             # Assuming you have an ngram analyzer defined in settings
#                             "analyzer": "your_ngram_analyzer"
#                         }
#                         # Add other sub-fields if necessary
#                     }
#                 }
#             }
#         },
#       "RISH": {
#         "type": "nested",
#         "properties": {
#           "license_long_name": { "type": "text" },
#           "license_short_name": { "type": "text" }
#         }
#       }
#     }
#   }
# }

INDEX_NAME="test9"
# Delete index if it exists
if es.indices.exists(index=INDEX_NAME):
    es.indices.delete(index=INDEX_NAME)


mapping = {
  "settings": {
    "index": {
      "max_ngram_diff": 10  # Important for ngram tokenizer
    },
    "analysis": {
      "tokenizer": {
        "my_ngram_tokenizer": {
          "type": "ngram",
          "min_gram": 2,
          "max_gram": 10,
          "token_chars": ["letter", "digit"]
        }
      },
      "filter": {
        "hebrew_stop": {
          "type": "stop",
          "stopwords": "_hebrew_"  # Built-in Hebrew stopwords
        }
      },
      "analyzer": {
        "hebrew_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "hebrew_stop"
          ]
        },
        "my_ngram_analyzer": {
          "type": "custom",
          "tokenizer": "my_ngram_tokenizer",
          "filter": [
            "lowercase"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "txt": {
        "type": "nested",
        "properties": {
          "entity_text_string": {
            "type": "text",
            "analyzer": "hebrew_analyzer",  # use the custom hebrew analyzer
            "fields": {
              "ngram": {
                "type": "text",
                "analyzer": "my_ngram_analyzer"
              },
              "keyword": {
                "type": "keyword"
              }
            }
          }
        }
      },
      "RISH": {
        "type": "nested",
        "properties": {
          "license_long_name": {
            "type": "text",
            "analyzer": "hebrew_analyzer",
            "fields": {
              "ngram": {
                "type": "text",
                "analyzer": "my_ngram_analyzer"
              },
              "keyword": {
                "type": "keyword"
              }
            }
          },
          "license_short_name": {
            "type": "text",
            "analyzer": "hebrew_analyzer",
            "fields": {
              "ngram": {
                "type": "text",
                "analyzer": "my_ngram_analyzer"
              },
              "keyword": {
                "type": "keyword"
              }
            }
          }
        }
      }
    }
  }
}




# --- Create the index ---
if not es.indices.exists(index=INDEX_NAME):
    # Create the index with the mapping
    try:
        es.indices.create(index=INDEX_NAME, body=mapping)
        print(f"Index '{INDEX_NAME}' created successfully with mapping.")
    except Exception as e:
        print(f"Error creating index '{INDEX_NAME}': {e}")
else:
    print(f"Index '{INDEX_NAME}' already exists.")
    # You might want to update the mapping if it's different from what you expect
    #  es.indices.put_mapping(index=INDEX_NAME, body=mapping['mappings']) #update the mapping
    #  print(f"Index '{INDEX_NAME}' already exists.  Mapping not updated.")
    pass




# Load JSON data from file
with open("data.json", "r", encoding="utf-8") as file:
    data = json.load(file)
    print('after load data')
# Index the documents
for doc in data:
    es.index(index=INDEX_NAME, id=doc["entity_id"], body=doc)

print("Indexing finished!")


search_text = "משלחת"
search_rish = "G-11/2018"

query_1 = {
  "query": {
    "bool": {
      "should": [
        {
          "nested": {
            "path": "txt",
            "query": {
              "bool": {
                "should": [
                  {
                    "match_phrase": {
                      "txt.entity_text_string": search_text
                    }
                  },
                  {
                    "match_phrase": {
                      "txt.entity_text_string.ngram": search_text
                    }
                  }
                ]
              }
            },
            "inner_hits": {}
          }
        },
        {
          "nested": {
            "path": "RISH",
            "query": {
              "bool": {
                "should": [
                  { "match": { "RISH.license_long_name": search_rish }},
                  { "match": { "RISH.license_long_name.ngram": search_rish }},
                  { "match": { "RISH.license_short_name": search_rish }},
                  { "match": { "RISH.license_short_name.ngram": search_rish }}
                ]
              }
            },
            "inner_hits": {}
          }
        }
      ]
    }
  }
}




search_text = "משלחת"

query_2 = {
  "query": {
    "nested": {
      "path": "txt",
      "query": {
        "bool": {
          "should": [
            {
              "match": {
                "txt.entity_text_string": {
                  "query": search_text,
                  "operator": "OR"
                }
              }
            },
            {
              "match": {
                "txt.entity_text_string.ngram": {
                  "query": search_text
                }
              }
            }
          ]
        }
      },
      "inner_hits": {}
    }
  }
}


query = {
  "query": {
    "nested": {
      "path": "txt",
      "query": {
        "match": {
          "txt.entity_text_string.ngram": {
            "query": search_text
          }
        }
      },
      "inner_hits": {}
    }
  }
}


# Execute search
response = es.search(index=INDEX_NAME, body=query)

# # Show results
for hit in response['hits']['hits']:
     print(hit["_source"])
     print("=" * 50)

print ("ok")
# ------------------
# Search Example:
# ------------------

# Search text
# search_text = "מטבע"  # or "משלחת לבד"
# search_rish = "A-3278/2000"

# query1 = {
#     "query": {
#         "bool": {
#             "should": [
#                 {
#                     "nested": {
#                         "path": "txt",
#                         "query": {
#                             "match_phrase": {  # <-- here!
#                                 "txt.entity_text_string": {
#                                     "query": search_text
#                                 }
#                             }
#                         },
#                         "inner_hits": {}
#                     }
#                 },
#                 {
#                     "nested": {
#                         "path": "RISH",
#                         "query": {
#                             "bool": {
#                                 "should": [
#                                     {
#                                         "match": {
#                                             "RISH.license_long_name": {
#                                                 "query": search_rish
#                                             }
#                                         }
#                                     },
#                                     {
#                                         "match": {
#                                             "RISH.license_short_name": {
#                                                 "query": search_rish
#                                             }
#                                         }
#                                     }
#                                 ]
#                             }
#                         },
#                         "inner_hits": {}
#                     }
#                 }
#             ]
#         }
#     }
# }

# query = {
#     "query": {
#         "nested": {
#             "path": "txt",
#             "query": {
#                 "match": {
#                     "txt.entity_text_string": {
#                         "query": "משלחת"
#                     }
#                 }
#             }
#         }
#     }
# }
# # Execute search
# response = es.search(index=index_name, body=query)

# # Show results
# for hit in response['hits']['hits']:
#     print(hit["_source"])
#     print("=" * 50)
#print ("ok")