const express = require("express");
const { Kafka } = require("kafkajs");

async function main() {
  const kafka = new Kafka({
    clientId: "users-consumer",
    brokers: ["localhost:9092", "localhost:9093"],
  });

  const consumer = kafka.consumer({ groupId: "usersView" });

  const eachMessage = async ({ message }) => {
    const type = message.key.toString();
    const data = JSON.parse(message.value.toString());

    switch (type) {
      case "userCreated": {
        _users.push(data);
        break;
      }
    }
  };

  await consumer.connect();
  await consumer.subscribe({ topic: "users", fromBeginning: true });
  await consumer.run({ eachMessage });

  const app = express();
  const _users = [];

  app.get("/users", (_req, res) => {
    res.json(_users);
  });

  app.get("/users/:userId", (req, res) => {
    const { userId } = req.params;
    res.json(_users.find((user) => user.id === userId));
  });

  app.listen(4000, () => {
    console.log(`Listening at port ${4000}`);
  });
}

main();
