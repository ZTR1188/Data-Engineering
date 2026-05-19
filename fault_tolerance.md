Before stopping broker 1



Topic: sensor-events	TopicId: LcE6UXJZQjy6cdtYUHzcOg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2

&#x09;Topic: sensor-events	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1

&#x09;Topic: sensor-events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2

&#x09;Topic: sensor-events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3



After stopping broker 1



Topic: sensor-events	TopicId: LcE6UXJZQjy6cdtYUHzcOg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2

&#x09;Topic: sensor-events	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3

&#x09;Topic: sensor-events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,2

&#x09;Topic: sensor-events	Partition: 2	Leader: 2	Replicas: 1,2,3	Isr: 2,3

