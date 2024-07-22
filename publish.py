from google.cloud import pubsub_v1
import json
import time

project_id = 'PROJECTID'
topics = {
    'ad_impressions': 'projects/PROJECTID/topics/ad_impressions',
    'clicks': 'projects/PROJECTID/topics/clicks',
    'conversions': 'projects/PROJECTID/topics/conversions',
    'bid_requests': 'projects/PROJECTID/topics/bid_requests'
}
publisher = pubsub_v1.PublisherClient()

def publish_messages():
    ad_impression_message = json.dumps({
        "ad_creative_id": "creative123",
        "user_id": "user456",
        "timestamp": int(time.time()),
        "website": "example.com"
    }).encode('utf-8')

    click_message = json.dumps({
        "timestamp": int(time.time()),
        "user_id": "user456",
        "ad_campaign_id": "campaign789",
        "conversion_type": None
    }).encode('utf-8')

    conversion_message = json.dumps({
        "timestamp": int(time.time()),
        "user_id": "user456",
        "ad_campaign_id": "campaign789",
        "conversion_type": "purchase"
    }).encode('utf-8')

    bid_request_message = json.dumps({
        "user_id": "user456",
        "auction_id": "auction123",
        "bid_amount": 1.23,
        "timestamp": int(time.time()),
    }).encode('utf-8')

    result = publisher.publish(topics['ad_impressions'], ad_impression_message).result()
    print(result)
    result=publisher.publish(topics['clicks'], click_message).result()
    print(result)
    result=publisher.publish(topics['conversions'], conversion_message).result()
    print(result)
    result=publisher.publish(topics['bid_requests'], bid_request_message).result()
    print(result)

publish_messages()
