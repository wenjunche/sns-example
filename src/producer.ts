
import { createTopic, publish } from "./aws-client";

export const producer = async () => {
  const topicArn = await createTopic('queueing-notifications-sandbox-dev');
  console.log(topicArn);

  await publish(topicArn, { ticker: 'ICE', price: Math.random() * 1000});  
};

producer();
