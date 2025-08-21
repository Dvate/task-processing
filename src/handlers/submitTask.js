import { DynamoDB, SQS } from 'aws-sdk';
const dynamodb = new DynamoDB.DocumentClient();
const sqs = new SQS();

const TABLE_NAME = process.env.TASKS_TABLE_NAME;
const QUEUE_URL = process.env.TASK_QUEUE_URL;

export async function handler(event) {
  try {
    const body = JSON.parse(event.body);
    if(!taskId || !payload) {
        response(400, { message: 'bad request' });
    }
    const { taskId, payload } = body;

    await dynamodb.put({
      TableName: TABLE_NAME,
      Item: {
        taskId,
        status: 'SUBMITTED',
        attempts: 0,
        payload,
        error: null,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      },
      ConditionExpression: 'attribute_not_exists(taskId)'
    }).promise();

    await sqs.sendMessage({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify({ taskId, payload })
    }).promise();

    return response(202, { message: 'Task accepted', taskId });
  } catch (err) {
    if (err.code === 'ConditionalCheckFailedException') {
      return response(409, { message: 'task already exists' });
    }
    console.error('submitTask error:', err);
    return response(500, { message: 'Internal server error' });
  }
}

function response(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  };
}