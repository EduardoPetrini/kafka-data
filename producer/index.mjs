import { Kafka } from "kafkajs";

const connectToKafka = () => {
  const options = {
    brokers: ["localhost:29092"],
    clientId: "kafka-js-client",
  };
  const kafka = new Kafka(options);
  return kafka.producer();
};

const send = async (producer) => {
  const resp = await producer.send({
    topic: 'a.b.c',
    messages: [
      { value: 'Hello KafkaJS user!' },
    ],
  });

  console.log(resp);
  return resp;
}

const start = async()=>{
  const producer = await connectToKafka();
  await producer.connect();
  await send(producer);
  await producer.disconnect();
}

start().then(()=> console.log('Done'))
.catch((err)=>{
  console.log(err);
  process.exit(1);
})