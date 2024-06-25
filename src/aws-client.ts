import { CreateTopicCommand, SNSClient, SubscribeCommand, paginateListTopics } from "@aws-sdk/client-sns";
import { CreateQueueCommand, SQSClient, paginateListQueues } from "@aws-sdk/client-sqs";

const snsClient = new SNSClient({});
const sqsClient = new SQSClient({});

export const getAllTopics = async (): Promise<string[]> => {
    const paginatedTopics = paginateListTopics({ client: snsClient }, {});
    const topics = [];
  
    for await (const page of paginatedTopics) {
      if (page.Topics?.length) {
        topics.push(...page.Topics);
      }
    }  
    return (topics.map((t) => t.TopicArn));
};

export const getAllQueues = async (): Promise<string[]> => {
    const paginatedQueues = paginateListQueues({ client: sqsClient }, {});
    const queues = [];
  
    for await (const page of paginatedQueues) {
      if (page.QueueUrls?.length) {
        queues.push(...page.QueueUrls);
      }
    }
    return queues;
  };
  
export const createTopic = async (topicName: string): Promise<string> => {
    const response = await snsClient.send(
      new CreateTopicCommand({ Name: topicName }),
    );
    console.log(response);
    // {
    //   '$metadata': {
    //     httpStatusCode: 200,
    //     requestId: '087b8ad2-4593-50c4-a496-d7e90b82cf3e',
    //     extendedRequestId: undefined,
    //     cfId: undefined,
    //     attempts: 1,
    //     totalRetryDelay: 0
    //   },
    //   TopicArn: 'arn:aws:sns:us-east-1:xxxxxxxxxxxx:TOPIC_NAME'
    // }
    if (response.$metadata.httpStatusCode !== 200) {
        throw new Error(`Error creating SNS topic ${response.$metadata.httpStatusCode}`);
    }
    return response.TopicArn;
};

export const createQueue = async (sqsQueueName: string) => {
  const command = new CreateQueueCommand({
    QueueName: sqsQueueName,
    Attributes: {
      DelaySeconds: "10",
      MessageRetentionPeriod: "300",
    },
  });
  const response = await sqsClient.send(command);
  console.log(response);
  return response;
};

export const subscribeQueue = async (topicArn: string, queueArn: string) => {
    const command = new SubscribeCommand({
      TopicArn: topicArn,
      Protocol: "sqs",
      Endpoint: queueArn,
    });  
    const response = await snsClient.send(command);
    console.log(response);
    // {
    //   '$metadata': {
    //     httpStatusCode: 200,
    //     requestId: '931e13d9-5e2b-543f-8781-4e9e494c5ff2',
    //     extendedRequestId: undefined,
    //     cfId: undefined,
    //     attempts: 1,
    //     totalRetryDelay: 0
    //   },
    //   SubscriptionArn: 'arn:aws:sns:us-east-1:xxxxxxxxxxxx:subscribe-queue-test-430895:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
    // }
    return response;
  };