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


mapping ={
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
      "entity_id": { "type": "integer" },
      "txt": {
        "type": "nested",
        "properties": {
          "entity_text_string": { "type": "text", "analyzer": "standard" }
        }
      }#,
      #"RISH": {
       # "type": "nested",
        #"properties": {
         # "license_long_name": { 
          #  "type": "text", 
           # "analyzer": "standard",
            #"fields": {
             # "keyword": { 
              #  "type": "keyword",
               # "normalizer": "lowercase_normalizer"
              #}
            #}
          #}
        #}
     # }
    }
  }
}
#drop Existing Index
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

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


query={
  "query": {
    "bool": {
      "must": [
       # {
        #  "nested": {
         #   "path": "RISH",
          #  "query": {
          #    "term": { "RISH.license_long_name.keyword": "A-3038/1999" }
           # }
         # }
        #},
        {
          "nested": {
            "path": "txt",
            "query": {
              #"match_phrase": { "txt.entity_text_string": "ירדנה" }  
              "match": { "txt.entity_text_string": "ירדנה" }  
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
print("\nEntity IDs containing 'ירדנה':")
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"])

query_1 = {
    "query": {
        "nested": {
            "path": "txt",
            "query": {
                "match_phrase": {
                    "txt.entity_text_string": {
                        "query": "משלחת ",
                        "slop": 2  # Allows up to 2 words in between
                    }
                }
            }
        }
    }
}

# Execute search query
response = es.search(index=index_name, body=query_1)

# Print entity_ids of matching documents
print("\nEntity IDs containing 'משלחת לבד':")
for hit in response["hits"]["hits"]:
    print(hit["_source"]["entity_id"])
    