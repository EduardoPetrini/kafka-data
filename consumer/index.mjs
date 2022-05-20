import { Kafka } from "kafkajs";

const connectToKafka = () => {
  const options = {
    brokers: ["localhost:29092"],
    clientId: "kafka-js-client",
  };
  const kafka = new Kafka(options);
  return kafka.consumer({ groupId: "kafka-js-client2" });
};

const listen = async (consumer) => {
  await consumer.subscribe({ topic: 'a.b.c', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}

const start = async()=>{
  const consumer = await connectToKafka();
  await consumer.connect();
  await listen(consumer);
  // await consumer.disconnect();
}

start().then(()=> console.log('Done'))
.catch((err)=>{
  console.log(err);
  process.exit(1);
})