"""
Real-time shopper event data generator
Simulates e-commerce user interactions for analytics pipeline
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
import boto3
from faker import Faker

fake = Faker()

class ShopperEventGenerator:
    def __init__(self):
        self.kinesis = boto3.client('kinesis')
        self.stream_name = 'shopper-events-stream'
        
        # Product categories for realistic simulation
        self.categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
        self.event_types = ['page_view', 'add_to_cart', 'purchase', 'search', 'ad_click']
        
    def generate_shopper_event(self) -> Dict:
        """Generate realistic shopper behavior event"""
        user_id = f"user_{random.randint(1000, 99999)}"
        session_id = fake.uuid4()
        
        event = {
            'event_id': fake.uuid4(),
            'user_id': user_id,
            'session_id': session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': random.choice(self.event_types),
            'product_id': f"prod_{random.randint(1, 10000)}",
            'category': random.choice(self.categories),
            'price': round(random.uniform(10, 500), 2),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'location': {
                'country': fake.country_code(),
                'city': fake.city(),
                'ip_address': fake.ipv4()
            },
            'ad_campaign_id': f"campaign_{random.randint(1, 100)}" if random.random() > 0.7 else None,
            'revenue': round(random.uniform(0, 500), 2) if random.choice(self.event_types) == 'purchase' else 0
        }
        
        return event
    
    def send_to_kinesis(self, event: Dict):
        """Send event to Kinesis stream"""
        try:
            response = self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(event),
                PartitionKey=event['user_id']
            )
            print(f"Sent event {event['event_id']} to Kinesis")
            return response
        except Exception as e:
            print(f"Error sending to Kinesis: {e}")
    
    def generate_batch_events(self, count: int = 1000):
        """Generate batch of events for testing"""
        events = []
        for _ in range(count):
            event = self.generate_shopper_event()
            events.append(event)
            
        # Save to local file for testing
        with open('sample_events.json', 'w') as f:
            json.dump(events, f, indent=2)
        
        print(f"Generated {count} sample events")
        return events

if __name__ == "__main__":
    generator = ShopperEventGenerator()
    generator.generate_batch_events(1000)