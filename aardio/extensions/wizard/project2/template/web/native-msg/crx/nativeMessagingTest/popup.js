// 这里的参数改为本地应用的标识名
var port = chrome.runtime.connectNative('com.my_company.my_application');

// 连接成功后的处理函数
function onConnected(port) {
  port.onMessage.addListener(onNativeMessage);
  port.onDisconnect.addListener(onDisconnected);
  document.getElementById('send-button').disabled = false;
}

// 本地应用消息处理函数
function onNativeMessage(msg) {
  const msgElement = document.getElementById("msg");
  if (msgElement) {
    msgElement.innerText = "本地应用发过来的对象：" + JSON.stringify(msg);
  }
}

// 断开连接处理函数
function onDisconnected() {
  const msgElement = document.getElementById("msg");
  if (msgElement) {
    msgElement.innerText = "已断开连接";
  }
  document.getElementById('send-button').disabled = true;
}

// 发送消息函数
function sendNativeMessage(message) {
  if (port) {
    port.postMessage(message);
  } else {
    console.error("无法发送消息，端口未连接");
  }
}

// 发送按钮点击事件处理
document.getElementById('send-button').addEventListener('click', () => {
  const messageInput = document.getElementById('message-input');
  const messageText = messageInput.value;
  if (messageText) {
    sendNativeMessage({ text: messageText });
    messageInput.value = ''; // 清空输入框
  } else {
    alert('请输入消息');
  }
});

// 错误处理
try {
  onConnected(port);
} catch (error) {
  console.error("连接本地应用时发生错误:", error);
}
