"""
Python script to convert each CSV row into a JSON payload and publish it to Kafka
"""
import csv
import json
import time
from kafka import KafkaProducer

# Initialize Kafka Producer targeting JSON serializer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k else None
)

def parse_array(value):
    """Splits comma-separated string into a list for ksqlDB ARRAY types."""
    if not value or value.strip() == "":
        return None
    return [item.strip() for item in value.split(',')]

def parse_float(value):
    """Safely converts string numbers to float/decimal."""
    try:
        return float(value) if value and value.strip() != "" else None
    except ValueError:
        return None

def parse_int(value):
    """Safely converts string numbers to integer."""
    try:
        return int(value) if value and value.strip() != "" else None
    except ValueError:
        return None

# Read and process CSV file
csv_file_path = 'your_hotel_reviews.csv'  # Update with your actual filename

with open(csv_file_path, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # Construct record matching ksqlDB reviews_stream schema
        payload = {
            "id": row.get("id"),
            "address": row.get("address") or None,
            "categories": parse_array(row.get("categories")),
            "primary_categories": parse_array(row.get("primary_categories")),
            "city": row.get("city") or None,
            "country": row.get("country") or None,
            "latitude": parse_float(row.get("latitude")),
            "longitude": parse_float(row.get("longitude")),
            "name": row.get("name") or None,
            "postal_code": row.get("postal_code") or None,
            "province": row.get("province") or None,
            "reviews_date": row.get("reviews_date") or None,  # Expects ISO format e.g. '2018-05-12T10:00:00Z'
            "reviews_rating": parse_int(row.get("reviews_rating")),
            "reviews_text": row.get("reviews_text") or None,
            "reviews_title": row.get("reviews_title") or None,
            "reviews_user_city": row.get("reviews_user_city") or None,
            "reviews_username": row.get("reviews_username") or None
        }

        # Send record using 'id' as key to topic US_hotel_reviews
        record_key = payload["id"]
        producer.send('US_hotel_reviews', key=record_key, value=payload)
        
        # Optional: Simulate 10 records per second (0.1s delay)
        time.sleep(0.1)

producer.flush()
print("CSV ingestion complete!")