from elasticsearch import Elasticsearch
print ('aaa')
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "12345678"
es = Elasticsearch("http://localhost:9200",basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),  # Authentication credentials
    verify_certs=True)
##es = Elasticsearch("http://10.255.7.254:9200")
#print(es.info())

# Define index mapping with geo-point field
index_name = "artifacts"

mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "text"},
            "location": {"type": "geo_point"}  # Geo-location field
        }
    }
}

# Create index (if it doesn't exist)
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully!")

# Sample artifacts with geo-coordinates
documents = [
    {"name": "Ancient Vase", "location": {"lat": 31.7683, "lon": 35.2137}},  # Jerusalem
    {"name": "Roman Sword", "location": {"lat": 32.0853, "lon": 34.7818}},  # Tel Aviv
    {"name": "Bronze Statue", "location": {"lat": 31.9500, "lon": 35.1000}},  # Near Jerusalem
    {"name": "Ancient Pottery", "location": {"lat": 41.9028, "lon": 12.4964}},  # Rome
]

# Index documents
for i, doc in enumerate(documents):
    es.index(index=index_name, id=i+1, body=doc)
print("Documents indexed successfully!")

# Define polygon for search (region in Israel)
polygon_points = [
    {"lat": 31.8000, "lon": 35.2000},
    {"lat": 31.8000, "lon": 35.2500},
    {"lat": 31.8500, "lon": 35.2500},
    {"lat": 31.8500, "lon": 35.2000}
]

# Geo-polygon query to find artifacts within the region
query = {
    "query": {
        "geo_polygon": {
            "location": {
                "points": polygon_points
            }
        }
    }
}

# Execute search query
response = es.search(index=index_name, body=query)

# Print results
print("\nArtifacts found within the polygon region:")
for hit in response["hits"]["hits"]:
    print(f"- {hit['_source']['name']} (Location: {hit['_source']['location']})")




query = {
    "query": {
        "artifacts": {
            "names": {
                "name": f"Roman Sword"
            }
        }
    }
}

# Execute search query
response = es.search(index=index_name, body=query)

# Print results
print("\n Artifacts found within the polygon region:")
#for hit in response["hits"]["hits"]:
 #   print(f"- {hit['_source']['name']} (names: {hit['_source']['name']})")
