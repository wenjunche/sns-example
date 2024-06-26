import { CreateTopicCommand, PublishCommand, SNSClient, SubscribeCommand, paginateListTopics } from "@aws-sdk/client-sns";
import { CreateQueueCommand, DeleteMessageCommand, GetQueueAttributesCommand, Message, ReceiveMessageCommand, SQSClient, paginateListQueues } from "@aws-sdk/client-sqs";

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

export const createQueue = async (sqsQueueName: string): Promise<string> => {
  const command = new CreateQueueCommand({
    QueueName: sqsQueueName,
    Attributes: {
      VisibilityTimeout: "0",
      DelaySeconds: "0",
      MessageRetentionPeriod: "300",
    },
  });
  const response = await sqsClient.send(command);
  console.log(response);
  return response.QueueUrl;
};

export const getQueueArn = async (queueUrl: string): Promise<string> => {
  const command = new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: ['QueueArn'],
  });

  const response = await sqsClient.send(command);
  console.log(response);
  // {
  //   '$metadata': {
  //     httpStatusCode: 200,
  //     requestId: '747a1192-c334-5682-a508-4cd5e8dc4e79',
  //     extendedRequestId: undefined,
  //     cfId: undefined,
  //     attempts: 1,
  //     totalRetryDelay: 0
  //   },
  //   Attributes: { DelaySeconds: '1' }
  // }
  return response.Attributes.QueueArn;
};

/**
 *For a SNS topic to be able to publish to a SQS queue,  the following needs to be added to Access Policy of the SQS queue
 *{
 *  "Statement": [
 *    {
 *      "Effect": "Allow",
 *      "Principal": {
 *        "Service": "sns.amazonaws.com"
 *      },
 *      "Action": "sqs:SendMessage",
 *      "Resource": "arn:aws:sqs:us-east-2:123456789012:MyQueue",
 *      "Condition": {
 *        "ArnEquals": {
 *          "aws:SourceArn": "arn:aws:sns:us-east-2:123456789012:MyTopic"
 *        }
 *      }
 *    }
 *  ]
 *}
 * 
 * @param topicArn 
 * @param queueArn 
 * @returns 
 */
export const subscribeQueue = async (topicArn: string, queueArn: string): Promise<string> => {
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
    return response.SubscriptionArn;
  };

  export const receiveMessage = async (queueUrl: string) => {
    console.log('waiting for messages', queueUrl);
    while (true) {
      const response = await sqsClient.send(
        new ReceiveMessageCommand({
          AttributeNames: ['All'],
          MaxNumberOfMessages: 1,
          MessageAttributeNames: ['All'],
          QueueUrl: queueUrl,
          WaitTimeSeconds: 20,
          VisibilityTimeout: 0,
        }),
      );

      if (response.Messages?.length > 0) {
        const body = JSON.parse(response.Messages[0].Body);
        const realMsg = JSON.parse(body.Message);
        console.log(realMsg.ticker, realMsg.price);
        deleteMessage(queueUrl, response.Messages[0]);
      } else {
        console.log('no messages');
      }
    }
}

export const deleteMessage = async (queueUrl:string, message: Message) => {
  console.log('deleting for messages', message.ReceiptHandle);
  const response = await sqsClient.send(
    new DeleteMessageCommand({
      QueueUrl: queueUrl,
      ReceiptHandle: message.ReceiptHandle
    }),
  );
  console.log(response);
}

export const publish = async (topicArn: string, message: unknown): Promise<string> => {
  const response = await snsClient.send(
    new PublishCommand({
      Message: JSON.stringify(message),
      TopicArn: topicArn,
    }),
  );
  console.log(response);
  // {
  //   '$metadata': {
  //     httpStatusCode: 200,
  //     requestId: 'e7f77526-e295-5325-9ee4-281a43ad1f05',
  //     extendedRequestId: undefined,
  //     cfId: undefined,
  //     attempts: 1,
  //     totalRetryDelay: 0
  //   },
  //   MessageId: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
  // }
  return response.MessageId;
};