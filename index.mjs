import { Kafka } from "kafkajs";

const connectToKafka = () => {
  const options = {
    brokers: ["localhost:29092"],
    clientId: "kafka-js-client",
  };
  const kafka = new Kafka(options);
  return kafka.admin();
};

const getTopicsFromKafka = async (admin) => {
  const topics = await admin.listTopics();

  console.log(topics);
  return topics;
};

const getTopicMetadata = async (admin, topic) => {
  const topicMetadata = await admin.fetchTopicMetadata({ topics: [topic] });
  const topicOffset = await admin.fetchTopicOffsets(topic);
  const consumerGroupOffset = await admin.fetchOffsets({ groupId: "kafka-js-client", topics: [topic] });

  return { topicMetadata, topicOffset, consumerGroupOffset };
};

const calcKafkaData = async (admin, topicList) => {
  const results = await topicList.reduce(
    async (acc, topic) => {
      const topicOffset = await admin.fetchTopicOffsets(topic);
      const partitionCount = topicOffset.length;
      const messageCount = topicOffset.reduce((sum, offset) => {
        sum += offset.high - offset.low
        return sum;
      }, 0);
      
      const newAcc = await acc;
      newAcc.greedyTopic = partitionCount > newAcc.greedyTopic ? partitionCount: newAcc.greedyTopic;
      newAcc.partitions.push(partitionCount);
      newAcc.messages.push(messageCount);

      return newAcc;
    },
    { greedyTopic: 0, partitions: [], messages: [] }
  );

  return results;
};

const medianMeanMode = (arr) => {
  const sortedArr = arr.sort((a, b) => a - b);
  const median = sortedArr[Math.floor(arr.length / 2)];
  const mean = arr.reduce((sum, val) => sum + val, 0) / arr.length;
  const mode = arr.reduce((acc, val) => {
    if (acc.count > val.count) {
      return acc;
    }
    return {
      value: val,
      count: 1,
    };
  }, { value: sortedArr[0], count: 1 });
  return { median, mean, mode };
}

const calcStatistics = (kafkaData)=>{
  const { partitions, messages } = kafkaData;
  const { median: medianPartitions, mean: meanPartitions, mode: modePartitions } = medianMeanMode(partitions);
  const { median: medianMessages, mean: meanMessages, mode: modeMessages } = medianMeanMode(messages);
  return { partitions, messages, medianPartitions, meanPartitions, modePartitions, medianMessages, meanMessages, modeMessages };
}

const start = async () => {
  const admin = await connectToKafka();
  await admin.connect();

  const topics = await getTopicsFromKafka(admin);
  const kafkaData = await calcKafkaData(admin, topics);
  const results = calcStatistics(kafkaData);

  console.log(results);
  await admin.disconnect();
};

start()
  .then(() => console.log("Done"))
  .catch((err) => {
    console.log(err);
    process.exit(1);
  });
