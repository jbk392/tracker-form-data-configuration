const date = new Date().toLocaleString("en-US", {timeZone: "America/New_York"}).split(',')[0].replaceAll('/', '-')
const MAX_PAYLOAD_SIZE = 256 * 1024 * 1024;
let devToolsConnection;
const requestQueue = [];
let processing = false;
let batchNumber = parseInt(1);
let logs = [];
let vmName = 'no-vm';

function enqueueRequest(requestData, vmName, urlName) {
  requestQueue.push({ requestData });
  if (!processing) {
    processQueue(vmName, urlName);
  }
}

async function processQueue(vmName, urlName) {
  if (requestQueue.length === 0 && logs.length === 0) {
    processing = false;
    return;
  }
  processing = true;
  const batch = requestQueue.splice(0, requestQueue.length); // Clear the queue into a batch
  let batchPayload = batch.map(({ requestData }) => ({ requestData }));

  let log_content = JSON.stringify(logs, null, 2);
  const logFile = {
    content: log_content,
    filename: `${date}/${vmName}/${urlName}/logs-${batchNumber}-mv3.json`,
    filetype: 'json'
  }
  batchPayload.push({requestData: logFile });  

  chrome.tabs.query({active: true, currentWindow: true}, tabs => {
    const message = {
      'action': 'download',
      'data': log_content,
      'batchNumber': batchNumber
    }
    // Forward the message to the content script in the active tab
    chrome.tabs.sendMessage(tabs[0].id, message);
  });

  fetch(ENV.UPLOAD_ENDPOINT, {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify(batchPayload)
  })
  .then(response => {
    requestType = 'successful-upload-response';
    logItem(ENV.UPLOAD_ENDPOINT, `upload-response for batch ${batchNumber}`, 'endpoint-response-success', `file count: ${batchPayload.length}`,  response.status, response.statusText);
  })
  .catch(error => {
    requestType = 'failed-upload-response';
    logItem(ENV.UPLOAD_ENDPOINT, `upload-response for batch ${batchNumber}`, 'endpoint-response-error', `file count: ${batchPayload.length}`, error.status, error);
  });

  batchNumber += 1;

  setTimeout(() => processQueue(vmName, urlName), 10000); // Process the next batch after 10 seconds
}

function logItem(url, filename, requestType, metadata, status) {
  let localBatchNumber = batchNumber;
  if (url == ENV.UPLOAD_ENDPOINT) {
    // the response from the batch upload will get uploaded itself in the subsequent batch
    localBatchNumber++
  }
  const newItem = {
    'timestamp': new Date().toISOString(),
    'url': url,
    'filename': filename,
    'requestType': requestType,
    'metadata': metadata,
    'status': status,
    'batch': localBatchNumber,
  }
  
  logs.push(newItem)
}

/* CONNECTORS & LISTENERS */
// listen for a connection from devtools
chrome.runtime.onConnect.addListener(function(port) {
  if (port.name == 'devtools-page') {
    devToolsConnection = port;
  }
});

setTimeout(() => {
  if (devToolsConnection) {
    devToolsConnection.postMessage({action: 'download-har'});
  }
}, 30000) 

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'file-to-download') {
    handleMessage(message);
  }
  if (message.action === 'download-har' && devToolsConnection) {
    devToolsConnection.postMessage({action: 'download-har'});
  }
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status === 'complete' && devToolsConnection) {
    devToolsConnection.postMessage({action: 'begin-downloads'});
  }
});

function getInstanceName() {
  return new Promise((resolve, reject) => {
    if (instanceName !== null) {
      resolve(instanceName);
    } else {
      fetch(chrome.runtime.getURL("./instance_name.txt"))
        .then(response => response.text())
        .then(data => {
          instanceName = data;
          resolve(instanceName);
        })
        .catch(reject);
    }
  });
}

function getUrlName() {
  return new Promise((resolve, reject) => {
    fetch(chrome.runtime.getURL("./url_name.txt"))
      .then(response => response.text())
      .then(data => {
        urlName = data;
        resolve(urlName);
      })
      .catch(reject);
  });
}

async function hashFilename(filename) {
  const encoder = new TextEncoder();
  const data = encoder.encode(filename);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  return hashHex.substring(0, 255);
}

async function handleMessage(incomingMessage) {
  // get this VM's ID, so we know which subdir to drop files into
  getInstanceName()
  .then(vmName => {
    getUrlName()
      .then(urlName => {
        hashFilename(incomingMessage.filename).then(hashedFilename => {
          logItem(incomingMessage.filename, hashedFilename, 'file-to-download', 'devtools.js', 'success');
          incomingMessage.filename = `${date}/${vmName}/${urlName}/${hashedFilename}`
          enqueueRequest(incomingMessage, vmName, urlName)
            if (incomingMessage.filetype != 'log') {              
              incomingMessage.content = pako.gzip(incomingMessage.content);
              incomingMessage.filename = `${date}/${vmName}/${urlName}/${hashedFilename}.gz`
              enqueueRequest(incomingMessage, vmName, urlName);
          }
        }) 
      });
  });
}