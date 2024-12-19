const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;

app.use(bodyParser.json());

// API endpoints
app.post('/tasks', (req, res) => {
    // 添加作业
});

app.get('/tasks', (req, res) => {
    res.json([{ id: 1, englishName: 'Task1', chineseName: '任务1' }]);
});

app.put('/tasks/:id', (req, res) => {
    // 更新作业
});

app.delete('/tasks/:id', (req, res) => {
    // 删除作业
});

app.post('/tasks/import', (req, res) => {
    // 批量导入作业
});

app.get('/tasks/export', (req, res) => {
    // 导出作业为xlsx文件
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
}); 