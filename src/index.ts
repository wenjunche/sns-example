
import { createTopic, getAllTopics } from "./aws-client";

export const helloSns = async () => {
  const topics = await getAllTopics();
  console.log(topics.join('\n'));

  const topicArn = await createTopic('queueing-notifications-sandbox-dev');
  console.log(topicArn);

};

helloSns();
