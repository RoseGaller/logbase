var ws;

function init() {
  // Connect to Web Socket.
  // Change host/port here to your own Web Socket server.
  ws = new WebSocket("ws://localhost:9003/logbase");

  // Set event handlers.
  ws.onopen = function() {
    output("onopen");
  };
  ws.onmessage = function(e) {
    // e.data contains received string.
    output("onmessage: " + e.data);
  };
  ws.onclose = function() {
    output("onclose");
  };
  ws.onerror = function() {
    output("onerror");
  };
}

function onSubmit() {
  console.log("CHECK")
  var input = document.getElementById("input");
  console.log("CHECK ", input.value)
//  ws.send(pack(1, input.value));
  ws.send(input.value);
  output("send: " + input.value);
  input.value = "";
  input.focus();
}

function onCloseClick() {
  ws.close();
}

function output(str) {
  var log = document.getElementById("log");
  var escaped = str.replace(/&/, "&amp;").replace(/</, "&lt;").
    replace(/>/, "&gt;").replace(/"/, "&quot;"); // "
  log.innerHTML = escaped + "<br>" + log.innerHTML;
}

function pack(cmd, str) {
  buflen = str.length + 1
  var buf = new ArrayBuffer(buflen);
  var bufView = new Uint8Array(buf);
  bufView[0] = cmd
  for (var i=1, strLen=str.length; i<buflen; i++) {
  	bufView[i] = str.charCodeAt(i-1);
   }
  return buf;
}
