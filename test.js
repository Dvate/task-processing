const axios = require("axios");
const { v4: uuidv4 } = require("uuid");

if (process.argv.length < 1) {
  console.error("Usage: node spam.js <endpoint-url> [count] [concurrency]");
  process.exit(1);
}

const endpoint = process.argv[2];
const totalRequests = parseInt(process.argv[3] || "100", 10);
const concurrency = parseInt(process.argv[4] || "1", 10);

async function sendTask(i) {
  const taskId = uuidv4();
  const payload = {
    taskId,
    payload: {
      foo: "bar",
      index: i,
      timestamp: new Date().toISOString(),
    },
  };

  try {
    const res = await axios.post(endpoint, payload, {
      headers: { "Content-Type": "application/json" },
      timeout: 5000,
    });
    console.log(`✅ Sent task ${taskId}, status: ${res.status}`);
  } catch (err) {
    console.error(
      `❌ Failed to send task ${taskId}:`,
      err.response?.status || err.message
    );
  }
}

async function main() {
  let i = 0;
  while (i < totalRequests) {
    const batch = [];
    for (let j = 0; j < concurrency && i < totalRequests; j++, i++) {
      batch.push(sendTask(i));
    }
    await Promise.all(batch);
  }
  console.log(`Finished sending ${totalRequests} tasks`);
}

main();
