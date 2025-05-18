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
mapping = {
    "mappings": {
        "properties": {
            "entity_id": {"type": "integer"},
            "txt": {
                "type": "nested",
                "properties": {
                    "entity_text_string": {"type": "text", "analyzer": "standard"}
                }
            },
            "RISH": {
               #"type": "nested",
                "properties": {
                    "license_long_name": {"type": "text", "analyzer": "standard"}
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

# Query to search for "ירושלים" in the 'txt.entity_text_string' field
query_1 = {
    "query": {
        "nested": {
            "path": "RISH",
            "query": {
                #"match": {"txt.entity_text_string": "מטבע"}
                #"match": {"txt.entity_text_string": "משלחת"}
                "match": {"RISH.license_long_name": "G-63"}
              #   "wildcard": {"txt.entity_text_string": "ירושלים"}  # Wildcard search
            }
        }
    }
}

# Execute search query
response_1 = es.search(index=index_name, body=query_1)
# Print entity_ids of matching documents
print("\nEntity IDs containing 'RISH':")
for hit in response_1["hits"]["hits"]:
    print(hit["_source"]["entity_id"])
#################################################################
query_2 = {
    "query": {
        "nested": {
            "path": "txt",
            "query": {
                #"match": {"txt.entity_text_string": "מטבע"}
                "match": {"txt.entity_text_string": "משלחת"}
              #   "wildcard": {"txt.entity_text_string": "ירושלים"}  # Wildcard search
            }
        }
    }
}

# Execute search query
response_1 = es.search(index=index_name, body=query_2)
# Print entity_ids of matching documents
print("\nEntity IDs containing 'משלחת':")
for hit in response_1["hits"]["hits"]:
    print(hit["_source"]["entity_id"])

#################################################################
query = {
    "query": {
        "nested": {
            "path": "txt",
            "query": {
                "match_phrase": {
                    "txt.entity_text_string": {
                        "query": "משלחת לבד",
                        "slop": 2  # Allows up to 2 words in between
                    }
                }
            }
        }
    }
}

# Execute search query
response = es.search(index=index_name, body=query)

# Print entity_ids of matching documents
print("\nEntity IDs containing 'משלחת לבד':")
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"])

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

print("Documents indexed successfully!")

query_3 = {
    "query": {
        "bool": {
            "should": [
                {"match": {"autocomplete_txt": "ירושל"}},  # Partial match
                {"match": {"full_txt": "ירושלים"}}  # Full match ranking
            ],
            "minimum_should_match": 1  # At least one match required
        }
    }
}


# Execute search
response = es.search(index=index_name, body=query_3)

# Print results
print("\nEntity IDs matching 'ירושל':")
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"], " | Score:", hit["_score"])

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

# Execute search
response = es.search(index="articles_hybrid", body=query_4)

# Print results
print("\nMatching entity_ids_4:")
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"], "| Score:", hit["_score"])


mapping = es.indices.get_mapping(index="articles_hybrid")
#print(mapping)
###################################################################################
index_name = "new_entity_indx"
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Index '{index_name}' deleted successfully!")
else:
    print(f"Index '{index_name}' does not exist.")

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
es.indices.create(index=index_name, body=mapping)
print(f"Index '{index_name}' created successfully!")



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