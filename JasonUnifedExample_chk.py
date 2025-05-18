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

# Define the index mapping
mapping1 = {
    "settings": {
        "index": {
            "max_ngram_diff": 20  # Important to allow ngram 2-20
        },
        "analysis": {
            "filter": {
                "autocomplete_filter": {
                    "type": "ngram",
                    "min_gram": 2,
                    "max_gram": 20
                }
            },
            "analyzer": {
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "autocomplete_filter"
                    ]
                },
                "autocomplete_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase"
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "entity_id": {"type": "integer"},
            "txt": {
                "type": "nested",
                "properties": {
                    "entity_text_string": {"type": "text", "analyzer": "standard"}
                }
            }
        }
    }
}

mapping2 = {
    "mappings": {
        "properties": {
            "entity_id": {"type": "integer"},
            "txt": {
                "type": "nested",
                "properties": {
                    "entity_text_string": {"type": "text", "analyzer": "standard"}
                }
            }
        }
    }
}

mapping = {
    "settings": {
        "index": {
            "max_ngram_diff": 20  # Important to allow ngram 2-20
        },
        "analysis": {
            "filter": {
                "autocomplete_filter": {
                    "type": "ngram",
                    "min_gram": 2,
                    "max_gram": 20
                }
            },
            "analyzer": {
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "autocomplete_filter"
                    ]
                },
                "autocomplete_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase"
                    ]
                }
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

# Create index (if it doesn't exist)
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully!")

# Helper function to clean text
def clean_text(text):
    if isinstance(text, str):
        return text.strip("'\"")  # remove leading/trailing ' or "
    return text  # if it's not a string, just return as is

# Recursive function to clean all text fields in nested dictionaries or lists
def clean_document(doc):
    if isinstance(doc, dict):
        return {k: clean_document(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [clean_document(item) for item in doc]
    else:
        return clean_text(doc)

# Load JSON data from file
with open("data.json", "r", encoding="utf-8") as file:
    data = json.load(file)
    print('after load data')

# Index the documents
#for doc in data:
 #   es.index(index=index_name, id=doc["entity_id"], body=doc)
#print("Documents indexed successfully!")
    
# # Index the documents
# for doc in data:
#     # Clean the txt.entity_text_string field
#     if "txt" in doc:
#         for item in doc["txt"]:
#             if "entity_text_string" in item:
#                 item["entity_text_string"] = clean_text(item["entity_text_string"])
    
#     # Now index the cleaned document
#     es.index(index=index_name, id=doc["entity_id"], body=doc)
    

    # Index the documents
for doc in data:
    cleaned_doc = clean_document(doc)  # <-- Clean the entire document
    es.index(index=index_name, id=cleaned_doc["entity_id"], body=cleaned_doc)

print("Indexing finished!!!")




search_text = "משלחת"
# Query to search for "ירושלים" in the 'txt.entity_text_string' field
query_1 = {
    "query": {
        "nested": {
            "path": "txt",
            "query": {
              #  "match": {"txt.entity_text_string": "מטבע"}
             #   "match": {"txt.entity_text_string": "משלחת"}
            #   "wildcard": {"txt.entity_text_string": "ירושלים"}  # Wildcard search
               "wildcard": {"txt.entity_text_string": search_text}  # Wildcard search
              #   "wildcard": {"txt.entity_text_string": "לים"}  # Wildcard search
            }
        }
    }
}

# Execute search query
response_1 = es.search(index=index_name, body=query_1)
# Print entity_ids of matching documents
print("\n query_1 Entity IDs containing "+search_text)
for hit in response_1["hits"]["hits"]:
    print(hit["_source"]["entity_id"])


search_text = "משלחת לבד"
#search_text = "משלחת"
query_2 = {
    "query": {
        "nested": {
            "path": "txt",
            "query": {
                "match_phrase": {
                    "txt.entity_text_string": {
                     #   "query": "משלחת לבד",
                        "query": search_text,
                        "slop": 2  # Allows up to 2 words in between
                    }
                }
            }
        }
    }
}




# Execute search query
response_2 = es.search(index=index_name, body=query_2)
# Print entity_ids of matching documents
print("\n query_2 Entity IDs containing "+search_text)
for hit in response_2["hits"]["hits"]:
    print(hit["_source"]["entity_id"])


search_text="מטבע"
#search_rish="G-11/2018"
search_rish="G-11"
query = {
  "query": {
    "bool": {
      "must": [
        {
          "nested": {
            "path": "txt",
            "query": {
              "match": {
                "txt.entity_text_string": {
                  "query": search_text,
                  "operator": "and"
                }
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
                  {
                    "term": {
                      "RISH.license_long_name":search_rish
                    }
                  },
                  {
                    "term": {
                      "RISH.license_short_name": search_rish
                    }
                  }
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

# Execute search query
response = es.search(index=index_name, body=query)

# Print entity_ids of matching documents
print("\n query -!!!! Entity IDs containing "+search_text)
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"])

#####################################################################
#####################################################################
search_text = ""
#search_text = "הבית השרוף"
#search_text ="ירושלים הבית השרוף"  #works with slop4
#search_text = "ירושלים העיר העתיקה"
#search_text = "ירושלים הרובע היהודי"  # works with slop2
#search_text = "הרובע היהודי הבית השרוף"  # doesn't works with slop2
#search_text = "ירושלים הבית השרוף"  #doesnt work even with slop 10
#search_text = "משלחת לבד"
#search_text = "משלחת"
search_text="מטבע"
search_rish="G-11/2018"
#search_rish="G-11"
#search_rish=""
#search_rish = "A-3278"
#search_thes = "ברונזה"
#search_thes = "מטבע, ברונזה"
search_thes = ""
search_thes_heb=""
#search_thes_heb = ["ברונזה", "מטבע"]  # your dynamic input list
#search_origin_table = "publications"
#search_origin_table = "licenses"
search_origin_table = ""

must_clauses = []

				
# if search_text:
#     must_clauses.append({
#         "nested": {
#             "path": "txt",
#             "query": {
#                 "match": {
#                     "txt.entity_text_string": {
#                         "query": search_text,
#                         "operator": "and"
#                     }
#                 }
#             },
#             "inner_hits": {}
#         }
#     })




if search_origin_table:
    must_clauses.append({
        "term": {
            "origin_table": search_origin_table
        }
    })


# for OR option 
if search_thes_heb:
    thes_should = []
    for val in search_thes_heb:
        thes_should.append({
            "match": {
                "thes.heb": val
            }
        })

    must_clauses.append({
        "nested": {
            "path": "thes",
            "query": {
                "bool": {
                    "should": thes_should,
                    "minimum_should_match": 1
                }
            },
            "inner_hits": {}
        }
    })

# for both values
#search_thes_heb = ["ברונזה", "מטבע"]

# must_clauses.append({
#     "nested": {
#         "path": "thes",
#         "query": {
#             "bool": {
#                 "must": [
#                     {"match": {"thes.heb": val}} for val in search_thes_heb
#                 ]
#             }
#         },
#         "inner_hits": {}  # Only one inner_hits block for the thes path
#     }
# })



# if search_thes:
#     must_clauses.append({
#         "nested": {
#             "path": "thes",
#             "query": {
#                 "match": {
#                     "thes.heb": {
#                         "query": search_thes,
#                         "operator": "and"
#                     }
#                 }
#             },
#             "inner_hits": {}
#         }
#     })

if search_text:
    must_clauses.append({
        "nested": {
            "path": "txt",
            "query": {
                "match_phrase": {
                    "txt.entity_text_string": {
                        "query": search_text,
                        "slop": 4
                        # "operator": "and"
                    }
                }
            },
            "inner_hits": {}
        }
    })

            
if search_rish:
    must_clauses.append({
        "nested": {
            "path": "RISH",
            "query": {
                "bool": {
                    "should": [
                        { "term": { "RISH.license_long_name": search_rish } },
                        { "term": { "RISH.license_short_name": search_rish } }
                    ]
                }
            },
            "inner_hits": {}
        }
    })

querym = {
    "query": {
        "bool": {
            "must": must_clauses
        }
    }
}
response = es.search(index=index_name, body=querym)

# Print entity_ids of matching documents
print("\n query MM -!!!! Entity IDs containing "+search_text+" "+search_rish+" "+search_thes+" "+search_origin_table)
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"])





   
print ("ok")
#####################################################################
#####################################################################

index_name = "articles_hybrid"


mapping = {
    "settings": {
        "analysis": {
            "tokenizer": {
                "edge_ngram_tokenizer": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 15,
                    "token_chars": ["letter", "digit"]
                }
            },
            "analyzer": {
                "edge_ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "edge_ngram_tokenizer",
                    "filter": ["lowercase"]
                },
                "standard_analyzer": {
                    "type": "standard"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "entity_id": {"type": "integer"},
            "autocomplete_txt": {
                "type": "text",
                "analyzer": "edge_ngram_analyzer",
                "search_analyzer": "standard"
            },
            "full_txt": {
                "type": "text",
                "analyzer": "standard_analyzer"
            }
        }
    }
}

# Create index
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully!")


# Index the documents
for doc in data:
    for txt_entry in doc.get("txt", []):
        text_string = txt_entry["entity_text_string"]
        es.index(index=index_name, id=doc["entity_id"], body={
            "entity_id": doc["entity_id"],
            "autocomplete_txt": text_string,
            "full_txt": text_string
        })

# print("Documents indexed successfully!")
search_text = "משלחת"
query_3 = {
    "query": {
        "bool": {
            "should": [
                {"match": {"autocomplete_txt": "שלחת"}},  # Partial match
              #  {"match": {"full_txt": "משלחת"}}  # Full match ranking
            ],
            "minimum_should_match": 1  # At least one match required
        }
    }
}


# Execute search
# response = es.search(index=index_name, body=query_3)
# print ("#######################################")
# # Print results
# print("\nEntity IDs matching 'ירושל':")
# for hit in response["hits"]["hits"]:
#     print(hit["_source"]["entity_id"], " | Score:", hit["_score"])

##############################################################################

query_4 = {
    "query": {
        "bool": {
            "must": [
                {"term": {"RISH.license_long_name.keyword": "A-3038/1999"}},  
                {
                   "nested": {
                   "path": "txt",
                    "query": {
                        #"match": {"txt.entity_text_string": "ירדנה"}
                          "match_phrase": {"txt.entity_text_string": "ירדנה"}
                        }
                    }
                }
            ]
        }
    }
}

# # Execute search
# response = es.search(index="articles_hybrid", body=query_4)

# # Print results
# print("\nMatching entity_ids_4:")
# for hit in response["hits"]["hits"]:
#     print(hit["_source"]["entity_id"], "| Score:", hit["_score"])


# mapping = es.indices.get_mapping(index="articles_hybrid")
# #print(mapping)
###################################################################################
# index_name = "new_entity_indx"
# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)
#     print(f"Index '{index_name}' deleted successfully!")
# else:
#     print(f"Index '{index_name}' does not exist.")

# Define the mapping
mapping = {
    "mappings": {
        "properties": {
            "RISH": {
                "type": "nested",
                "properties": {
                    "license_long_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    }
                }
            },
            "txt": {
                "type": "nested",
                "properties": {
                    "entity_text_string": {"type": "text"}
                }
            }
        }
    }
}

# Create the index with the mapping
# es.indices.create(index=index_name, body=mapping)
# print(f"Index '{index_name}' created successfully!")



query_5 = {
    "query": {
        "bool": {
            "must": [
                {"term": {"RISH.license_long_name.keyword": "A-3038/1999"}},  
                {
                   "nested": {
                   "path": "txt",
                    "query": {
                        #"match": {"txt.entity_text_string": "ירדנה"}
                          "match_phrase": {"txt.entity_text_string": "ירדנה"}
                        }
                    }
                }
            ]
        }
    }
}