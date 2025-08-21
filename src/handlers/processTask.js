"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();

const TABLE = process.env.TASKS_TABLE;
const BACKOFF_BASE = parseInt(process.env.BACKOFF_BASE_SECONDS || "5", 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);

exports.handler = async (event) => {
  await Promise.all(
    event.Records.map(async (record) => {
      const messageId = record.messageId;
      const receiptHandle = record.receiptHandle;

      const body = JSON.parse(record.body);

      const { taskId } = body;
      let attempts;

      try {
        const data = await dynamodb
          .update({
            TableName: TABLE,
            Key: { taskId },
            UpdateExpression:
              "SET #status = :s, #updatedAt = :u, ADD #attempts :inc",
            ExpressionAttributeNames: {
              "#status": "status",
              "#updatedAt": "updatedAt",
              "#attempts": "attempts",
            },
            ExpressionAttributeValues: {
              ":s": "PROCESSING",
              ":u": new Date().toISOString(),
              ":inc": 1,
            },
            ConditionExpression: "attribute_exists(taskId)",
            ReturnValues: "ALL_NEW",
          })
          .promise();
        attempts = data.Attributes.attempts;
      } catch (err) {
        // task not found in db, log taskId and skip processing
        if (err.code === "ConditionalCheckFailedException") {
          console.error(`Task ${taskId} not found in db, skipping processing`);
        }
      }

      // lets simulate processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      const shouldFail = Math.random() < 0.3;

      if (!shouldFail) {
        // Success path
        await dynamodb
          .update({
            TableName: TABLE,
            Key: { taskId },
            UpdateExpression: "SET #status = :s, #updatedAt = :u",
            ExpressionAttributeNames: {
              "#status": "status",
              "#updatedAt": "updatedAt",
            },
            ExpressionAttributeValues: {
              ":s": "COMPLETED",
              ":u": new Date().toISOString(),
            },
          })
          .promise();
        console.log(
          `Task ${taskId} processed successfully (messageId ${messageId}).`
        );
        return;
      }

      // Max retries hit: route to DLQ
      if (attempts >= MAX_RETRIES) {
        console.warn(
          `Task ${taskId} reached max retries (${MAX_RETRIES}). Routing to DLQ. messageId=${messageId}`
        );
        await dynamodb
          .update({
            TableName: TABLE,
            Key: { taskId },
            UpdateExpression:
              "SET #status = :s, #updatedAt = :u",
            ExpressionAttributeNames: {
              "#status": "status",
              "#updatedAt": "updatedAt",
            },
            ExpressionAttributeValues: {
              ":s": "FAILED_FINAL",
              ":u": new Date().toISOString(),
            },
          })
          .promise();

          // we rely on maxReceiveCount to drive message to DLQ. So after visibility timeout message will be moved there automatically
          return;
      }

      // Retry flow
      try {
        const delay = BACKOFF_BASE * attempts;
        await sqs
          .changeMessageVisibility({
            QueueUrl: process.env.TASK_QUEUE_URL,
            ReceiptHandle: receiptHandle,
            VisibilityTimeout: delay,
          })
          .promise();
      } catch (err) {
        console.error("Failed to change visibility timeout:", err);
      }

      try {
        await dynamodb
          .update({
            TableName: TABLE,
            Key: { taskId },
            UpdateExpression:
              "SET #status = :s, #updatedAt = :u",
            ExpressionAttributeNames: {
              "#status": "status",
              "#updatedAt": "updatedAt",
            },
            ExpressionAttributeValues: {
              ":s": "FAILED_PENDING",
              ":u": new Date().toISOString(),
            },
          })
          .promise();
      } catch (err) {
        console.error("Failed to update db record after failure:", err);
      }
    })
  );
};
