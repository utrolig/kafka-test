const express = require("express");
const { json: jsonParser } = require("body-parser");
const { Kafka } = require("kafkajs");
const { v4: uuid } = require("uuid");

async function main() {
  const kafka = new Kafka({
    clientId: "users-producer",
    brokers: ["localhost:9092", "localhost:9093"],
  });

  const producer = kafka.producer();
  await producer.connect();

  const app = express();
  app.use(jsonParser());

  app.post("/users", async ({ body }, res) => {
    const user = {
      id: uuid(),
      name: body.name,
      age: body.age,
    };

    const event = {
      topic: "users",
      messages: [
        {
          key: "userCreated",
          value: JSON.stringify(user),
        },
      ],
    };

    await producer.send(event);

    res.json(user);
  });

  app.listen(3000, () => {
    console.log(`Listening at port ${3000}`);
  });
}

main();
