import { CreateQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, SetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";
const client=new SQSClient({region:process.env.AWS_REGION??"us-east-1",endpoint:process.env.SQS_ENDPOINT??"http://localhost:4566",credentials:{accessKeyId:"test",secretAccessKey:"test"}});
const dlqName=process.env.SQS_DLQ_NAME??"wager-transactions-dlq.fifo";
const mainName=process.env.SQS_QUEUE_NAME??"wager-transactions.fifo";
const outboxName=process.env.OUTBOX_QUEUE_NAME??"wager-events.fifo";
await client.send(new CreateQueueCommand({QueueName:dlqName,Attributes:{FifoQueue:"true",ContentBasedDeduplication:"false"}}));
const dlq=await client.send(new GetQueueUrlCommand({QueueName:dlqName}));
if(!dlq.QueueUrl)throw new Error("SQS_DLQ_URL_NOT_FOUND");
const dlqAttributes=await client.send(new GetQueueAttributesCommand({QueueUrl:dlq.QueueUrl,AttributeNames:["QueueArn"]}));
if(!dlqAttributes.Attributes?.QueueArn)throw new Error("SQS_DLQ_ARN_NOT_FOUND");
await client.send(new CreateQueueCommand({QueueName:mainName,Attributes:{FifoQueue:"true",ContentBasedDeduplication:"false"}}));
const main=await client.send(new GetQueueUrlCommand({QueueName:mainName}));
if(!main.QueueUrl)throw new Error("SQS_MAIN_URL_NOT_FOUND");
await client.send(new SetQueueAttributesCommand({QueueUrl:main.QueueUrl,Attributes:{RedrivePolicy:JSON.stringify({deadLetterTargetArn:dlqAttributes.Attributes.QueueArn,maxReceiveCount:process.env.SQS_MAX_RECEIVE_COUNT??"5"})}}));
await client.send(new CreateQueueCommand({QueueName:outboxName,Attributes:{FifoQueue:"true",ContentBasedDeduplication:"false"}}));
for(const name of [dlqName,mainName,outboxName]){const r=await client.send(new GetQueueUrlCommand({QueueName:name}));console.log(`${name}: ${r.QueueUrl}`)}
