var port = chrome.runtime.connect({name: "devtools-page"});
port.onMessage.addListener(function(msg) {
  if (msg.action === 'begin-downloads') {
    beginDownloads();
  }
  if (msg.action == 'download-har') {
    downloadHar();
  }
});

function beginDownloads() {
  chrome.devtools.inspectedWindow.getResources(async (resources) => {
    resources.forEach((resource, index, array) => {
      resource.getContent(function(content, encoding) {

        if (!resource.url.includes("chrome_extension")) {
          let uploadContent = content;
          if (content === null || content === '') {
            uploadContent = 'file has no content';
          }
          console.log("uploading content: ", uploadContent);
          incomingMessage = {
            'content': uploadContent,
            'filename': resource.url,
            'action': 'file-to-download',
            'filetype': resource.type
          }
          chrome.runtime.sendMessage(incomingMessage);
        }
      })
    });
  });
}

function downloadHar() {
  chrome.devtools.network.getHAR(
    function (harLog) {
        // notify context script to download file
        const message = {
          'action': 'file-to-download', 
          'filetype': 'har',
          'content': JSON.stringify(harLog),
          'filename': `har`
        };
        chrome.runtime.sendMessage(message);
    }
  );

}