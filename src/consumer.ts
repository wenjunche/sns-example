
import { createQueue, createTopic, getAllTopics, getQueueArn, receiveMessage, subscribeQueue } from "./aws-client";

export const consumer = async () => {
  const topics = await getAllTopics();
  console.log(topics.join('\n'));

  const topicArn = await createTopic('queueing-notifications-sandbox-dev');
  console.log(topicArn);

  const queueUrl = await createQueue('queueing-notifications-sandbox-dev');
  console.log(queueUrl);

  const queueArn = await getQueueArn(queueUrl);
  console.log(queueArn);

  const subArn = await subscribeQueue(topicArn, queueArn);
  console.log('SubscriptionArn', subArn);

  await receiveMessage(queueUrl);
  
};

consumer();
