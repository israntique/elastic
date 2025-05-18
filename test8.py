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

# Define the index name
index_name = "entities"

# Full Mapping with NGRAM and max_ngram_diff
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
                },
                 "ngram_analyzer": {
                    "tokenizer": "custom_ngram_tokenizer",
                     "filter": ["lowercase"] # Good practice, though Hebrew is unicase
                }
            },
            "tokenizer": {
                  "custom_ngram_tokenizer": {
                  "type": "ngram",
                  "min_gram": 2,         # Minimum length of characters in a gram
                  "max_gram": 10,        # Maximum length of characters in a gram
                  "token_chars": [
                      "letter",
                      "digit"
                   ]
                }
           }
        }
    },
    "mappings": {
        "properties": {
            "entity_id": { "type": "long" },
            "txt": {
                "type": "nested",
                "properties": {
                    "entity_text_string": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer",
                        "search_analyzer": "autocomplete_search_analyzer",
                        "fields": {
                        "ngram": {        # Sub-field for partial matching
                            "type": "text",
                            "analyzer": "ngram_analyzer" # Use custom ngram analyzer
                        }
                        }
                    }
                }
            }
        }
    }
}

# Delete old index if exists
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Old index '{index_name}' deleted.")

# Create the index
es.indices.create(index=index_name, body=mapping)
print(f"Index '{index_name}' created successfully.")

# Load your data from a file (or create sample data manually)
with open("data.json", "r", encoding="utf-8") as file:
    data = json.load(file)
    print('Data loaded.')

# Index the documents
for doc in data:
    es.index(index=index_name, id=doc["entity_id"], document=doc)

print("Documents indexed successfully!")

# Example Search (partial word)
search_text = "משלחת"  # partial word, like part of "משלחת"

# Build the nested query
query = {
    "query": {
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
    }
}
# Execute the search
response = es.search(index=index_name, body=query)

# Print the results
print("\nEntity IDs matching your search EXCAVATION 1:")
for hit in response['hits']['hits']:
    entity_id = hit['_source']['entity_id']
    print(f"Found entity_id: {entity_id}")

################################################################

search_term="משלחת"
query_body = {
    "query": {
        "nested": {
            "path": "txt",  # The path to your nested field
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                # Query the standard field (Hebrew analyzer)
                                "txt.entity_text_string": {
                                    "query": search_term,
                                    "boost": 2.0 # Optional: Rank full matches higher
                                }
                            }
                        },
                        {
                            "match": {
                                # Query the ngram sub-field (Ngram analyzer)
                                "txt.entity_text_string": {
                                     "query": search_term
                                 }
                            }
                        }
                    ],
                    "minimum_should_match": 1 # At least one clause must match
                }
            }
            # Optional: Add inner_hits to see which nested object matched
            ,"inner_hits": {}
        }
    }
}

# Execute the search
response = es.search(index=index_name, body=query_body)

# Print the results
print("\nEntity IDs matching your search EXCAVATION2:")
for hit in response['hits']['hits']:
    entity_id = hit['_source']['entity_id']
    print(f"Found entity_id: {entity_id}")
