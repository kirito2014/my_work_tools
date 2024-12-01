<?php
// 启用错误报告
error_reporting(E_ALL);
ini_set('display_errors', 1);

// 允许跨域访问（如果需要的话）
header('Access-Control-Allow-Origin: *');
header('Content-Type: application/json; charset=utf-8');

/**
 * 卡片文字API调用类 - 使用一言API
 */
class CardTextApi {
    private $apiUrl = 'https://v1.hitokoto.cn/?c=f&encode=text';
    private $cardCount = 4;

    public function getCardTexts() {
        $results = [];
        
        for ($i = 0; $i < $this->cardCount; $i++) {
            $response = $this->makeApiRequest();
            if ($response) {
                $results[] = [
                    'title' => $this->generateTitle($response),
                    'content' => $response
                ];
            } else {
                error_log("Failed to get response from Hitokoto API for card $i");
            }
        }

        if (empty($results)) {
            throw new Exception('无法从一言API获取数据');
        }

        return $results;
    }

    private function makeApiRequest() {
        $ch = curl_init();
        
        $options = [
            CURLOPT_URL => $this->apiUrl,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => 10,
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
            CURLOPT_USERAGENT => 'Mozilla/5.0',
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false
        ];
        
        curl_setopt_array($ch, $options);
        
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        
        if ($response === false) {
            error_log('Curl error: ' . curl_error($ch));
            curl_close($ch);
            return false;
        }
        
        if ($httpCode !== 200) {
            error_log("HTTP Code: $httpCode, Response: $response");
            curl_close($ch);
            return false;
        }
        
        curl_close($ch);
        return $response;
    }

    private function generateTitle($content) {
        return mb_substr($content, 0, 4, 'UTF-8');
    }
}

// 处理请求
try {
    $api = new CardTextApi();
    $cardTexts = $api->getCardTexts();
    
    echo json_encode([
        'success' => true,
        'data' => $cardTexts
    ], JSON_UNESCAPED_UNICODE);
    
} catch (Exception $e) {
    error_log('API Error: ' . $e->getMessage());
    echo json_encode([
        'success' => false,
        'message' => $e->getMessage()
    ]);
} 