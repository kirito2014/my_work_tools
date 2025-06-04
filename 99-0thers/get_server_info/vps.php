<?php
// 服务器状态测试脚本
$veid = "1632978";
$api_key = "private_HgVQsmwXZrpIOTTUFvKaCtsN";

// 设置请求URL
$request = "https://api.64clouds.com/v1/getServiceInfo?veid={$veid}&api_key={$api_key}";

// 获取服务器信息
try {
    // 使用cURL获取数据
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $request);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false); // 仅测试使用，生产环境应启用验证
    curl_setopt($ch, CURLOPT_TIMEOUT, 10);
    
    $response = curl_exec($ch);
    
    if (curl_errno($ch)) {
        throw new Exception("cURL错误: " . curl_error($ch));
    }
    
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    if ($httpCode !== 200) {
        throw new Exception("API请求失败，HTTP状态码: {$httpCode}");
    }
    
    curl_close($ch);
    
    // 解码JSON响应
    $serviceInfo = json_decode($response);
    
    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new Exception("JSON解析错误: " . json_last_error_msg());
    }
    
    // 检查API返回的错误
    if (isset($serviceInfo->error) && $serviceInfo->error !== 0) {
        throw new Exception("API返回错误: " . ($serviceInfo->message ?? '未知错误'));
    }
    
    // 格式化输出
    echo "<!DOCTYPE html>
    <html>
    <head>
        <title>服务器状态测试结果</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            pre { background: #f5f5f5; padding: 15px; border-radius: 5px; }
            .success { color: green; }
            .error { color: red; }
        </style>
    </head>
    <body>
        <h1>服务器状态测试结果</h1>
        <p>API请求URL: <code>{$request}</code></p>";
    
    if ($serviceInfo) {
        echo "<p class='success'>API请求成功!</p>
              <h3>服务器信息:</h3>
              <pre>" . print_r($serviceInfo, true) . "</pre>";
    } else {
        echo "<p class='error'>未获取到有效数据</p>";
    }
    
    echo "</body></html>";
    
} catch (Exception $e) {
    // 错误处理
    echo "<!DOCTYPE html>
    <html>
    <head>
        <title>服务器状态测试 - 错误</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .error { color: red; }
            pre { background: #f5f5f5; padding: 15px; border-radius: 5px; }
        </style>
    </head>
    <body>
        <h1>服务器状态测试 - 错误</h1>
        <p>API请求URL: <code>{$request}</code></p>
        <p class='error'>错误: " . htmlspecialchars($e->getMessage()) . "</p>";
    
    if (isset($response)) {
        echo "<h3>原始响应:</h3>
              <pre>" . htmlspecialchars($response) . "</pre>";
    }
    
    echo "</body></html>";
}
?>