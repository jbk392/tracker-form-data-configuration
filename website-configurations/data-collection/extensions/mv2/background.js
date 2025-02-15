const requestQueue = [];
let processing = false;
const date = new Date().toLocaleString("en-US", {timeZone: "America/New_York"}).split(',')[0].replaceAll('/', '-')
let firstLoad = true; // only inject the form once
let instanceName = null; // only load the instance name once 
let seen_content = new Set();
let batchNumber = parseInt(1);
let logs = [];
let tabId = null;
let webNavigationComplete = false;


/****** HELPER FUNCTIONS ***** */
// Function to add requests to the queue
function enqueueRequest(requestData, vmName, urlName) {
  requestQueue.push({ requestData });
  if (!processing) {
    processQueue(vmName, urlName);
  }
}

// Function to process the queue
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
    filename: `${date}/${vmName}/${urlName}/logs-${batchNumber}.json`,
    filetype: 'json'
  }
  batchPayload.push({requestData: logFile });
  
  batchNumber += 1;


  fetch(ENV.UPLOAD_ENDPOINT, {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify(batchPayload)
  })
  .then(response => {
    requestType = 'successful-upload-response';
    logItem(ENV.UPLOAD_ENDPOINT, `upload-response for batch ${batchNumber}`, requestType, `file count: ${batchPayload.length}`, response);
  })
  .catch(error => {
    requestType = 'failed-upload-response';
    logItem(ENV.UPLOAD_ENDPOINT, `upload-response for batch ${batchNumber}`, requestType, `file count: ${batchPayload.length}`, error);
  });

  

  setTimeout(() => processQueue(vmName, urlName), 10000); // Process the next batch after 10 seconds
}

// get the instance name from the instance_name.txt file
// use this to name the directory where the files will be stored
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

// get the instance name from the url_name.txt file
// use this to name the directory where the files will be stored
// this will be written by the coordinator python script as it loads each url
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

async function getHash(filename) {
  const encoder = new TextEncoder();
  const data = encoder.encode(filename);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  return hashHex.substring(0, 255);
}

/* 
We want to log the following information:
1) any time we see a network request or response, incl. timestamp and metadata
2) when we hear back from the content script about form submission
3) when we attempt to download an HTML file
4) when we hear back from our upload endpoint

*/
async function logItem(url, filename, requestType, metadata, status, incomingMessage) {
  if (!url.startsWith("chrome-extension://")) {
    getInstanceName().then(vmName => {
      getUrlName().then(urlName => {
        getHash(filename).then(hashedName => {
  
          // handle logging
          let localBatchNumber = batchNumber;
          if (url == ENV.UPLOAD_ENDPOINT) {
            // the response from the batch upload will get uploaded itself in the subsequent batch
            localBatchNumber++
          }
          const newItem = {
            'timestamp': new Date().toISOString(),
            'url': url,
            'filename': hashedName,
            'requestType': requestType,
            'metadata': metadata,
            'status': status,
            'batch': batchNumber,
          }
          logs.push(newItem)
  
          // add file to upload queue with new filename
          if (incomingMessage != false && incomingMessage != undefined) {
            incomingMessage.filename = `${date}/${vmName}/${urlName}/${hashedName}`;
            enqueueRequest(incomingMessage, vmName, urlName);
          }
        })
      })
    })
  }
}
/****** END HELPER FUNCTIONS ***** */


/****** UPLOAD FUNCTIONS ***** */
// prepare file path for upload
/* incomingMessage takes the format of: 
{
    content,
    filename,
    filetype
}
*/
async function handleMessage(incomingMessage) {
  // get this VM's name, so we know which subdir to drop files into
  getInstanceName()
    .then(vmName => {
      getUrlName()
        .then(urlName => {
            getHash(incomingMessage.filename).then(hashedName => {
                let newname = hashedName;
                if (incomingMessage.filename == 'html') {
                    newname = incomingMessage.filename;
                }
                let encodedContent = '';
                try {
                  encodedContent = btoa(incomingMessage.content);
                } catch(error) {
                  encodedContent = btoa(encodeURIComponent(incomingMessage.content).replace(/%([0-9A-F]{2})/g, function(match, p1) {
                    return String.fromCharCode('0x' + p1);
                  }));
                }
                if (!seen_content.has(encodedContent)) {
                  seen_content.add(encodedContent);
                  incomingMessage.filename = `${date}/${vmName}/${urlName}/${newname}`;
                  
                  enqueueRequest(incomingMessage, vmName, urlName);
                }
            });
         
        });
    });
}
/****** END UPLOAD FUNCTIONS ***** */


/****** COORDINATOR FUNCTIONS ***** */
// receive notification that the HTML form has downloaded
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'form-submitted') {
    console.log("form-submitted");
    logItem('form-submitted', 'form-submitted', 'successful-form-submit', '', 'success', false)
  }

  if (message.action === 'failed-to-inject-form') {
    console.log("failed to inject form")
    logItem('failed-to-inject-form', 'failed-to-inject-form', 'failed-to-inject-form', message.data['error'], 'error', false)
  }

  if (message.action === 'logItem') {
    console.log("logging from content script: ", message);
    logItem(message.data.url, message.data.filename, message.data.requestType, message.data.metadata, message.data.status, false)
  }
});

chrome.webNavigation.onCompleted.addListener((details) => {
  console.log("Navigation completed for tab:", details.tabId);
  attachDebugger(details.tabId);
  tabId = details.tabId
  if (firstLoad) {
    firstLoad = false;
    logItem('page-loaded-web-navigation', 'page-loaded', 'page-loaded', '', 'success', false);
    console.log("Notifying content script...");
    chrome.tabs.sendMessage(details.tabId, {action: 'page-loaded'}, (response) => {
      if (chrome.runtime.lastError) {
        console.error("Error sending message:", chrome.runtime.lastError.message);
      } else {
        webNavigationComplete = true;
      }
    });
  }
}, {url: [{urlMatches : 'http://*/*'}, {urlMatches : 'https://*/*'}]});

setTimeout(() => {
  if (!firstLoad || !webNavigationCompleted) {
    chrome.tabs.sendMessage(tabId, {action: 'page-loaded'});
  }
}, 30000) 
/****** END COORDINATOR FUNCTIONS ***** */


/**** START DEBUGGING FUNCTIONS *****/
function attachDebugger(tabId) {
  chrome.debugger.attach({tabId: tabId}, '1.3', () => {
      chrome.debugger.sendCommand({tabId: tabId}, 'Network.enable');
  });
}

chrome.tabs.query({}, (tabs) => {
  for (const tab of tabs) {
      attachDebugger(tab.id);
  }
});

chrome.tabs.onCreated.addListener((tab) => {
  attachDebugger(tab.id);
});

function logNetworkFile(method, paramsItem, status) {
    const filename = paramsItem.url;
    logItem(paramsItem.url, filename, method, paramsItem, status, false)
}

function handleNetworkResponse(response, responseBody, requestId) {
  let filename = response.url;
  const responsePacket = {
    content: responseBody === '' ? 'No content received' : responseBody,
    filename: `${filename}-${requestId}`,
    filetype: 'js',
    url: response.url
  };
  logItem(filename, filename, 'network-response', 'network response', 'success', responsePacket)
} 

chrome.debugger.onEvent.addListener((source, method, params) => {
  if (method === 'Network.requestWillBeSent') {
    logNetworkFile(method, params.request, 'success', false);
  }

  if (method === 'Network.responseReceived') {
      const response = params.response;
      logNetworkFile(method, params.response, 'success', false);

      chrome.debugger.sendCommand(
          {tabId: source.tabId},
          'Network.getResponseBody',
          {requestId: params.requestId},
          (responseBody) => {
              if (!responseBody) {
                logNetworkFile(chrome.runtime.lastError, params.request, 'error', false);
              } else {
                handleNetworkResponse(response, responseBody.body, params.requestId);
              }
          }
      );
  }

  if (method === 'Network.loadingFailed') {
      logItem(params.request.url, 'loading-failed', 'loading-failed', params.errorText, method, false);
  }

  if (method === 'Network.loadingFinished') {
      logItem(params.request.url, 'loading-finished', 'loading-finished', 'loading-finished', method, false); 
  }
});

chrome.debugger.onDetach.addListener((source, reason) => {
  logItem(source.tabId, 'debugger-detached', 'debugger-detached', reason, 'debugger-detached');
});

chrome.webRequest.onBeforeRequest.addListener(
  function(details) {
    let filename = details.url;
    logItem(details.url, 'not-fetched', filename, 'on-before-request', false);
    return { cancel: false };
  },
  {
    urls: ["<all_urls>"]
  },
  ["blocking"]
);