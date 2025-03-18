from kafka import KafkaConsumer, TopicPartition

# Kafka Configuration
bootstrap_servers = "localhost:9092"  # Update with your broker address
topic = "your_topic_name"
group_id = "your_consumer_group"  # Ensure this matches your actual consumer group

# Create a consumer
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    auto_offset_reset="earliest",
    enable_auto_commit=False
)

# Get partitions for the topic
partitions = consumer.partitions_for_topic(topic)
if partitions is None:
    print(f"Topic '{topic}' does not exist or has no partitions.")
else:
    for partition in partitions:
        tp = TopicPartition(topic, partition)
        
        # Assign the partition to the consumer before fetching offsets
        consumer.assign([tp])
        
        # Fetch the last committed offset (only works if offsets were committed)
        committed_offset = consumer.committed(tp)

        # Fetch the latest available offset
        end_offset = consumer.end_offsets([tp])[tp]

        # Fetch timestamp for the last committed offset
        committed_timestamp = None
        if committed_offset is not None:
            offset_timestamp_info = consumer.offsets_for_times({tp: committed_offset})
            if offset_timestamp_info and tp in offset_timestamp_info and offset_timestamp_info[tp]:
                committed_timestamp = offset_timestamp_info[tp].timestamp

        print(f"Partition {partition}:")
        print(f"  - Last committed offset: {committed_offset}")
        print(f"  - Last available offset: {end_offset}")
        print(f"  - Timestamp of last committed offset: {committed_timestamp}")

# Close the consumer
consumer.close()
