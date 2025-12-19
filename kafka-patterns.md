### The Five Core Kafka Patterns (Covers ~80% of Data Engineering Interview Questions)

Research shows that approximately 80% of FAANG data engineering interviews on Apache Kafka focus on five repeatable patterns. Mastering these is more valuable than attempting random Kafka topics.

#### Pattern 1: Producer Basics & Message Publishing

**Why FAANG tests this**: Every data engineering system needs to ingest data reliably. Understanding producer semantics, configurations, and failure modes is fundamental to building robust data pipelines.

**What you need to know**:
- Producer configuration (batch size, linger time, compression)
- Synchronous vs asynchronous message sending
- Callbacks and error handling
- Message partitioning and key selection
- Idempotent and transactional producers for exactly-once semantics
- Performance tuning and throughput optimization

**Practice focus areas**:
- Choosing appropriate key for partitioning
- Handling producer timeouts and retries
- Batch vs single message publishing trade-offs
- Monitoring producer metrics (throughput, latency, errors)
- Ensuring message ordering guarantees

**Code snippet - Basic Producer**:
```python
from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'my_producer',
    'acks': 'all',  # Wait for all replicas
    'compression.type': 'snappy'
})

data = {'user_id': 123, 'event': 'login', 'timestamp': 1702500000}
producer.produce(
    topic='user_events',
    key=str(data['user_id']).encode('utf-8'),
    value=json.dumps(data).encode('utf-8'),
    callback=delivery_report
)

producer.flush()  # Ensure all messages are sent
```

**Code snippet - Exactly-Once Semantics**:
```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'my_transaction_id',
    'enable.idempotence': True,
    'acks': 'all'
})

producer.init_transactions()

try:
    producer.begin_transaction()
    for i in range(100):
        producer.produce('transactions', f'message_{i}'.encode())
    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
    print(f'Transaction failed: {e}')
```

**Real-world scenarios**: Event streaming, log aggregation, data ingestion pipelines, real-time analytics

---

#### Pattern 2: Consumer Basics & Offset Management

**Why FAANG tests this**: Building reliable consumers is critical for data pipelines. Understanding offset commits, consumer lag, and failure recovery prevents data loss and duplicates.

**What you need to know**:
- Consumer groups and partitions assignment
- Offset commit strategies (auto, manual, hybrid)
- At-least-once vs at-most-once vs exactly-once consumption
- Consumer lag monitoring
- Rebalancing and pause/resume operations
- Idempotent consumer design for exactly-once processing

**Practice focus areas**:
- Manual offset commits for critical data
- Handling offset out of range errors
- Monitoring consumer lag and alert thresholds
- Graceful shutdown and offset persistence
- Dealing with slow message processing

**Code snippet - Basic Consumer**:
```python
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'user_events_group',
    'auto.offset.reset': 'earliest',  # Start from beginning if no offset found
    'enable.auto.commit': False,  # Manual offset management
    'session.timeout.ms': 6000
})

consumer.subscribe(['user_events'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue
        
        # Process message
        event = json.loads(msg.value().decode('utf-8'))
        print(f'Received: {event}')
        
        # Manual commit after successful processing
        consumer.commit(asynchronous=False)
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

**Code snippet - Manual Offset Management with Error Handling**:
```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'data_pipeline_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

consumer.subscribe(['events'])

def process_message(msg):
    """Process message, return True if successful"""
    try:
        data = json.loads(msg.value().decode('utf-8'))
        # Business logic here
        return True
    except Exception as e:
        print(f'Processing failed: {e}')
        return False

while True:
    msg = consumer.poll(timeout=1.0)
    
    if msg is None:
        continue
    
    if msg.error():
        continue
    
    if process_message(msg):
        consumer.commit(asynchronous=False)
    else:
        # On error, pause and seek back to retry
        consumer.pause(consumer.assignment())
        consumer.seek(msg)
        time.sleep(5)  # Wait before retry
        consumer.resume(consumer.assignment())
```

**Real-world scenarios**: Data warehouse ETL, stream processing, real-time dashboards, event-driven systems

---

#### Pattern 3: Partitioning & Consumer Groups

**Why FAANG tests this**: Understanding partition assignment and consumer group dynamics is essential for scaling data pipelines. This determines throughput, latency, and load balancing.

**What you need to know**:
- How partitions enable parallelism
- Partition assignment strategies (RoundRobin, Range, Sticky, Cooperative-Sticky)
- Consumer group rebalancing and its impact
- Optimal partition count selection
- Scaling consumers dynamically
- Message ordering guarantees with partitioning

**Practice focus areas**:
- Choosing partition key to ensure load balance
- Minimizing rebalancing impact with sticky assignment
- Handling single-threaded vs multi-threaded consumption
- Coordinating state across multiple consumer instances
- Debugging partition lag skew

**Code snippet - Understanding Partition Assignment**:
```python
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'analysis_group',
    'auto.offset.reset': 'earliest',
    'partition.assignment.strategy': 'cooperative-sticky'
})

def on_assign(consumer, partitions):
    """Called when partitions are assigned"""
    print(f'Assigned partitions: {partitions}')
    for partition in partitions:
        print(f'  Topic: {partition.topic()}, Partition: {partition.partition()}')

def on_revoke(consumer, partitions):
    """Called when partitions are revoked (before rebalancing)"""
    print(f'Revoked partitions: {partitions}')

consumer.subscribe(['events'], on_assign=on_assign, on_revoke=on_revoke)

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    print(f'Message from partition {msg.partition()}: {msg.value()}')
```

**Code snippet - Partition-Specific Processing**:
```python
# Process specific partitions with dedicated consumers
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest'
})

from confluent_kafka import TopicPartition

# Manually assign specific partitions (no consumer group)
partitions = [TopicPartition('events', 0), TopicPartition('events', 1)]
consumer.assign(partitions)

# Seek to specific offset
consumer.seek(TopicPartition('events', 0, 1000))

while True:
    msg = consumer.poll(timeout=1.0)
    if msg:
        print(f'Partition {msg.partition()}, Offset {msg.offset()}: {msg.value()}')
```

**Real-world scenarios**: Distributed processing, load balancing, hot partition handling, multi-tenant data segregation

---

#### Pattern 4: Error Handling & Exactly-Once Processing

**Why FAANG tests this**: Data loss or duplication has serious business consequences. Understanding error scenarios, recovery strategies, and exactly-once semantics is critical for mission-critical systems.

**What you need to know**:
- Delivery semantics: at-most-once, at-least-once, exactly-once
- Idempotent producer configuration and design
- Transactional writes and atomic state management
- Dead letter queues for poison messages
- Circuit breakers and graceful degradation
- Retry logic with exponential backoff
- Consumer offset commit timing

**Practice focus areas**:
- Implementing retry logic with backoff
- Designing idempotent message handlers
- Handling poison messages
- Coordinating offset commits with message processing
- Monitoring and alerting on error rates

**Code snippet - Retry Logic with Exponential Backoff**:
```python
import time
from confluent_kafka import Consumer
import json

def process_with_retry(msg, max_retries=3):
    """Process message with exponential backoff retry logic"""
    for attempt in range(max_retries):
        try:
            data = json.loads(msg.value().decode('utf-8'))
            # Simulate processing that might fail
            result = perform_expensive_operation(data)
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                print(f'Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}')
                time.sleep(wait_time)
            else:
                print(f'Max retries exceeded: {e}')
                return False
    return False

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'resilient_group',
    'enable.auto.commit': False
})

consumer.subscribe(['events'])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None or msg.error():
        continue
    
    if process_with_retry(msg):
        consumer.commit(asynchronous=False)
    else:
        # Send to dead letter queue
        send_to_dlq(msg)
        consumer.commit(asynchronous=False)
```

**Code snippet - Exactly-Once with External State**:
```python
# Common pattern: read offset from database before processing
import psycopg2
from confluent_kafka import TopicPartition

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest'
})

def get_last_processed_offset(partition_id):
    """Retrieve last processed offset from database"""
    conn = psycopg2.connect("dbname=pipeline user=processor")
    cur = conn.cursor()
    cur.execute("SELECT offset FROM partition_offsets WHERE partition_id = %s", (partition_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else 0

# Resume from database offset (exactly-once guarantee)
partition_id = 0
offset = get_last_processed_offset(partition_id)
consumer.assign([TopicPartition('events', partition_id, offset)])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None or msg.error():
        continue
    
    # Process and persist atomically with offset
    process_message(msg)
    save_offset_to_db(msg.partition(), msg.offset())
```

**Real-world scenarios**: Financial transactions, audit logs, critical data pipelines, compliance-sensitive systems

---

#### Pattern 5: Monitoring, Performance Tuning & Operational Resilience

**Why FAANG tests this**: In production, operational excellence matters as much as correctness. Understanding metrics, bottlenecks, and scaling strategies separates junior from senior engineers.

**What you need to know**:
- Consumer lag monitoring and alerting
- Producer throughput and latency metrics
- Broker health and replica synchronization
- Network and CPU bottlenecks
- Batch size and compression trade-offs
- Multi-threaded consumption patterns
- Graceful shutdown and zero-loss operations

**Practice focus areas**:
- Setting appropriate lag thresholds for alerts
- Benchmarking producer/consumer throughput
- Identifying and resolving bottlenecks
- Scaling consumer threads vs multiple processes
- Circuit breaker patterns for downstream failures
- Capacity planning and cost optimization

**Code snippet - Consumer Lag Monitoring**:
```python
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'monitoring_group',
    'enable.auto.commit': True
})

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

def get_consumer_lag(group_id, topic):
    """Calculate consumer lag for a topic"""
    consumer.subscribe([topic])
    time.sleep(1)  # Wait for assignment
    
    # Get consumer committed offsets
    committed_offsets = consumer.committed(consumer.assignment())
    
    # Get topic end offsets (high water mark)
    end_offsets = admin.list_offsets(
        {TopicPartition(topic, p.partition()): 
         OffsetSpec.latest() for p in consumer.assignment()}
    )
    
    total_lag = 0
    for partition in consumer.assignment():
        committed = committed_offsets[partition.partition()].offset
        end = end_offsets[partition].offsets[0][0]
        lag = end - committed if committed >= 0 else end
        total_lag += lag
        print(f'Partition {partition.partition()}: lag = {lag}')
    
    return total_lag

lag = get_consumer_lag('monitoring_group', 'events')
print(f'Total consumer lag: {lag}')
```

**Code snippet - Optimized High-Throughput Consumer**:
```python
from confluent_kafka import Consumer
import json
from concurrent.futures import ThreadPoolExecutor

# Producer configuration tuned for throughput
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'batch.size': 32768,  # 32KB batches
    'linger.ms': 100,     # Wait up to 100ms for batching
    'compression.type': 'snappy',
    'acks': 1             # Leader ack only for speed
}

# Consumer configuration for high throughput
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'high_throughput_group',
    'fetch.min.bytes': 102400,    # 100KB min batch
    'fetch.max.wait.ms': 500,     # Wait up to 500ms
    'max.poll.records': 1000,     # Fetch up to 1000 messages
    'session.timeout.ms': 30000
}

consumer = Consumer(consumer_config)
consumer.subscribe(['events'])

def process_batch(messages):
    """Process batch of messages in parallel"""
    for msg in messages:
        if msg.error():
            continue
        data = json.loads(msg.value().decode('utf-8'))
        # Perform processing
        yield data

with ThreadPoolExecutor(max_workers=4) as executor:
    while True:
        messages = consumer.consume(num_messages=100, timeout=1.0)
        if messages:
            results = list(executor.map(lambda m: process_batch([m]), messages))
```

**Real-world scenarios**: High-volume stream processing, real-time dashboards, fraud detection, recommendation engines, IoT data ingestion

---

### Quick Reference: Production Checklist

- [ ] **Producers**: Set `acks=all`, enable idempotence, configure retries
- [ ] **Consumers**: Manual offset commits, error handling, graceful shutdown
- [ ] **Partitioning**: Select appropriate partition keys, monitor lag per partition
- [ ] **Monitoring**: Consumer lag, rebalance latency, end-to-end latency
- [ ] **Resilience**: Dead letter queues, retry logic, circuit breakers
- [ ] **Performance**: Tune batch sizes, monitor throughput, profile bottlenecks
- [ ] **Operational**: Document offset reset procedures, maintain runbooks, test disaster recovery
