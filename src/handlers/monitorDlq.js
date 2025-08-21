const { DynamoDB } = require("aws-sdk");
const dynamodb = new DynamoDB.DocumentClient();

const TABLE_NAME = process.env.TASKS_TABLE_NAME;

exports.handler = async (event) => {
  for (const record of event.Records) {
    const body = JSON.parse(record.body);
    const { taskId, payload } = body || {};

    const { Item } = await dynamodb
      .get({ TableName: TABLE_NAME, Key: { taskId } })
      .promise();

    const { attempts, status, error } = Item || {};

    console.error("Task landed to DLQ:", {
      taskId,
      attempts,
      payload,
      status,
      error,
      dlqMessageId: record.messageId,
      sentTimestamp: record.attributes?.SentTimestamp,
    });
  }
};
