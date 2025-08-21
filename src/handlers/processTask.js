const { DynamoDB, SQS } = require("aws-sdk");
const dynamodb = DynamoDB.DocumentClient();
const sqs = new SQS();

const TABLE_NAME = process.env.TASKS_TABLE_NAME;
const BACKOFF_BASE = parseInt(process.env.BACKOFF_BASE_SECONDS || "5", 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
const FAILURE_RATE = parseFloat(process.env.FAILURE_RATE || "0.3");

exports.handler = async (event) => {
  const batchItemFailures = [];

  const results = await Promise.allSettled(
    event.Records.map(async (record) => {
      try {
        await processRecord(record);
        return { success: true, messageId: record.messageId };
      } catch (err) {
        console.error(`Failed to process record ${record.messageId}:`, err);
        batchItemFailures.push({ itemIdentifier: record.messageId });
        return { success: false, messageId: record.messageId, err };
      }
    })
  );

  const successful = results.filter((r) => r.value?.success).length;
  const failed = results.filter((r) => !r.value?.success).length;
  console.log(
    `Batch processing complete: ${successful} successful, ${failed} failed`
  );

  // Return batch failure information for SQS partial batch failure handling
  return {
    batchItemFailures,
  };
};

async function processRecord(record) {
  const messageId = record.messageId;
  const body = JSON.parse(record.body);
  const { taskId } = body;
  let attempts;

  try {
    // Update task status to PROCESSING and increment attempts
    const data = await dynamodb
      .update({
        TableName: TABLE_NAME,
        Key: { taskId },
        UpdateExpression:
          "SET #status = :s, #updatedAt = :u ADD #attempts :inc",
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
    if (err.code === "ConditionalCheckFailedException") {
      console.error(`Task ${taskId} not found in db, skipping processing`);
      return;
    }
    throw err; // Re-throw other errors to trigger retry
  }

  // Lets emulate some processing
  await new Promise((resolve) => setTimeout(resolve, 1000));

  const shouldFail = Math.random() < FAILURE_RATE;

  if (!shouldFail) {
    // Success path
    await dynamodb
      .update({
        TableName: TABLE_NAME,
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
      `Task ${taskId} completed successfully (messageId: ${messageId})`
    );
    return;
  }

  // Failure path
  if (attempts > MAX_RETRIES) {
    console.warn(
      `Task ${taskId} reached max retries (${MAX_RETRIES}). Will be sent to DLQ. messageId=${messageId}`
    );

    await dynamodb
      .update({
        TableName: TABLE_NAME,
        Key: { taskId },
        UpdateExpression: "SET #status = :s, #updatedAt = :u, #error = :e",
        ExpressionAttributeNames: {
          "#status": "status",
          "#updatedAt": "updatedAt",
          "#error": "error",
        },
        ExpressionAttributeValues: {
          ":s": "FAILED_FINAL",
          ":u": new Date().toISOString(),
          ":e": `Max retries reached: ${attempts}`,
        },
      })
      .promise();

    // maxReceiveCount on SQS will force message to drop into DLQ
    throw new Error(
      `Task ${taskId} failed permanently after ${attempts} attempts`
    );
  }

  // Retry path
  await dynamodb
    .update({
      TableName: TABLE_NAME,
      Key: { taskId },
      UpdateExpression: "SET #status = :s, #updatedAt = :u, #error = :e",
      ExpressionAttributeNames: {
        "#status": "status",
        "#updatedAt": "updatedAt",
        "#error": "error",
      },
      ExpressionAttributeValues: {
        ":s": "FAILED_PENDING",
        ":u": new Date().toISOString(),
        ":e": `Processing failed, attempt ${attempts}`,
      },
    })
    .promise();

  const delay = BACKOFF_BASE * Math.pow(2, attempts - 1);

  try {
    await sqs
      .changeMessageVisibility({
        QueueUrl: TASK_QUEUE_URL,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: delay,
      })
      .promise();

    console.log(
      `Task ${taskId} failed (attempt ${attempts}), will retry (messageId: ${messageId})`
    );
  } catch (err) {
    console.error(`Failed to set visibility timeout for ${taskId}:`, err);
    throw new Error(`Task ${taskId} processing failed on attempt ${attempts}`);
  }
}
