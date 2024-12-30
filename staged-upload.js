import dotenv from "dotenv";
import axios from "axios";
import fs from "fs"; // For reading the file
import FormData from "form-data";
import path from "path";
import stream from "stream";
dotenv.config(); // Load environment variables

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION = process.env.SHOPIFY_API_VERSION || "2024-07";

const endpoint = `https://${SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql.json`;

// Example JSON Data
const metafields = [
  {
    key: "color",
    namespace: "product_info",
    ownerId: "gid://shopify/Product/9818390004021",
    type: "single_line_text_field",
    value: "Red",
  },
  {
    key: "size",
    namespace: "product_info",
    ownerId: "gid://shopify/Product/9818390004021",
    type: "single_line_text_field",
    value: "Medium",
  },
  {
    key: "material",
    namespace: "product_info",
    ownerId: "gid://shopify/Product/9818390004021",
    type: "single_line_text_field",
    value: "Cotton",
  },
];
const filePath = path.join(
  "C:",
  "Users",
  "PC",
  "Documents",
  "WorkSpace",
  "Appio",
  "data",
  "bulk_op_vars.jsonl"
);
const filePathForMetafield = path.join(
  "C:",
  "Users",
  "PC",
  "Documents",
  "WorkSpace",
  "Appio",
  "data",
  "bulk_metafield.jsonl"
);
const stagedUploadMutation = `
  mutation {
    stagedUploadsCreate(input: { resource: BULK_MUTATION_VARIABLES, filename: "bulk_op_vars", mimeType: "text/jsonl", httpMethod: POST }) {
      userErrors {
        field
        message
      }
      stagedTargets {
        url
        resourceUrl
        parameters {
          name
          value
        }
      }
    }
  }
`;
// Convert JSON to JSONL format
function convertToJSONL(data) {
  return data.map((item) => JSON.stringify({ metafields: [item] })).join("\n");
}

async function runBulkOperation(stagedUploadPath) {
  const bulkMutation = `
    mutation {
      bulkOperationRunMutation(
        mutation: "mutation call($input: ProductInput!) { productCreate(input: $input) { product {id title variants(first: 10) {edges {node {id title inventoryQuantity }}}} userErrors { message field } } }",
        stagedUploadPath: "${stagedUploadPath}"
      ) {
        bulkOperation {
          id
          url
          status
        }
        userErrors {
          message
          field
        }
      }
    }
  `;

  const headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
  };

  const response = await axios.post(
    endpoint,
    { query: bulkMutation },
    { headers }
  );

  console.log("Bulk Operation Product Response:", response.data);
  // Extract userErrors from the response
  const userErrors = response.data?.data?.bulkOperationRunMutation?.userErrors;
  // Log the user errors if any
  if (userErrors && userErrors.length > 0) {
    console.error("User Errors:", JSON.stringify(userErrors, null, 2));
  } else {
    console.log("No errors, operation successful.");
  }
}

async function runBulkOperationForMetafield(stagedUploadPath) {
  const bulkMutation = `
    mutation {
      bulkOperationRunMutation(
        mutation: "mutation call($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
        metafields {
            key
            namespace
            value
            createdAt
            updatedAt
        }
    }
}",
        stagedUploadPath: "${stagedUploadPath}"
      ) {
        bulkOperation {
          id
          status
          errorCode
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
  };

  const response = await fetch(endpoint, {
    method: "POST",
    headers,
    body: JSON.stringify({ query: bulkMutation }),
  });

  const data = await response.json();
  console.log("Bulk Operation Response:", data);
  if (data.errors) {
    console.error("Errors:", JSON.stringify(data.errors, null, 2));
  } else {
    console.log("No errors, operation successful.");
  }
}
async function uploadAndRun() {
  try {
    // 1. Create staged upload
    const headers = {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": ACCESS_TOKEN,
    };

    const stagedUploadResponse = await axios.post(
      endpoint,
      { query: stagedUploadMutation },
      { headers }
    );

    const data =
      stagedUploadResponse.data.data.stagedUploadsCreate.stagedTargets[0];
    console.log(data);
    // 2. Upload file
    const form = new FormData();

    data.parameters.forEach((param) => {
      form.append(param.name, param.value);
    });

    form.append("file", fs.createReadStream(filePathForMetafield));

    await axios.post(data.url, form, {
      headers: {
        ...form.getHeaders(),
      },
    });

    console.log("File uploaded successfully!");

    const stagedUploadPath = data.parameters.find(
      (param) => param.name === "key"
    ).value;
    await runBulkOperationForMetafield(stagedUploadPath);
  } catch (error) {
    console.error(
      "Error:",
      error.response ? error.response.data : error.message
    );
  }
}

async function uploadAndRunUsingJson() {
  try {
    // 1. Create staged upload
    const headers = {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": ACCESS_TOKEN,
    };

    const stagedUploadResponse = await fetch(endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify({ query: stagedUploadMutation }),
    });

    const stagedUploadData = await stagedUploadResponse.json();
    const data = stagedUploadData.data.stagedUploadsCreate.stagedTargets[0];
    console.log(data);

    // 2. Upload file
    const jsonlData = convertToJSONL(metafields);
    const bufferStream = new stream.PassThrough();
    bufferStream.end(jsonlData);

    const form = new FormData();

    data.parameters.forEach((param) => {
      form.append(param.name, param.value);
    });

    form.append("file", bufferStream, {
      filename: "metafields.jsonl",
      contentType: "text/plain; charset=utf-8",
    });

    await axios.post(data.url, form, {
      headers: {
        ...form.getHeaders(),
      },
    });
    console.log("File uploaded successfully!");

    const stagedUploadPath = data.parameters.find(
      (param) => param.name === "key"
    ).value;
    await runBulkOperationForMetafield(stagedUploadPath);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

let webhookId = null; // Global variable to store webhook ID

async function createWebhook() {
  const checkWebhookMutation = `
    query {
      webhookSubscriptions(first: 10, topics: BULK_OPERATIONS_FINISH) {
        edges {
          node {
            id
            topic
            callbackUrl
          }
        }
      }
    }
  `;

  const headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
  };

  try {
    // Step 1: Check if a webhook already exists for the topic
    const checkWebhookResponse = await axios.post(
      endpoint,
      { query: checkWebhookMutation },
      { headers }
    );

    const existingWebhooks =
      checkWebhookResponse.data.data.webhookSubscriptions.edges;

    // Step 2: If a webhook for this topic exists, skip creation
    if (existingWebhooks.length > 0) {
      console.log("Webhook already exists for BULK_OPERATIONS_FINISH topic.");
      webhookId = existingWebhooks[0].node.id; // Use the existing webhook ID
      console.log("Using existing webhook ID:", webhookId);
      return; // Exit early
    }

    // Step 3: If no webhook exists, create a new one
    const webhookMutation = `
      mutation {
        webhookSubscriptionCreate(
          topic: BULK_OPERATIONS_FINISH
          webhookSubscription: {
            format: JSON,
            callbackUrl: "https://demonss0910.npkn.net/1d67b6"
          }
        ) {
          userErrors {
            field
            message
          }
          webhookSubscription {
            id
          }
        }
      }
    `;

    const response = await axios.post(
      endpoint,
      { query: webhookMutation },
      { headers }
    );

    const webhookData = response.data.data.webhookSubscriptionCreate;
    if (webhookData.userErrors.length > 0) {
      console.error("Webhook Errors:", webhookData.userErrors);
      return;
    }

    webhookId = webhookData.webhookSubscription.id;
    console.log("Webhook Created. ID:", webhookId);
  } catch (error) {
    console.error(
      "Error creating webhook or checking for existing webhook:",
      error.response ? error.response.data : error.message
    );
  }
}

uploadAndRunUsingJson().then(() => createWebhook());
