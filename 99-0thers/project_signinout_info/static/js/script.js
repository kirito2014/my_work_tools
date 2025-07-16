// 全局变量
let currentRange = 'today'; // 默认时间范围
let trendChart, distributionChart, overtimeChart, batchChart;

// 初始化图表
function initCharts() {
    // 签到趋势图
    trendChart = echarts.init(document.getElementById('trend-chart'));
    // 签到分布图
    distributionChart = echarts.init(document.getElementById('distribution-chart'));
    // 加班时长图
    overtimeChart = echarts.init(document.getElementById('overtime-chart'));
    // 签到批次环形图
    batchChart = echarts.init(document.getElementById('batch-chart'));
    // 签到状态环形图

    
    // 设置初始配置
    trendChart.setOption(getTrendOption([]));
    distributionChart.setOption(getDistributionOption([]));
    overtimeChart.setOption(getOvertimeOption([]));
    batchChart.setOption(getBatchOption([]));

}

// 获取概览数据
function fetchOverview(range) {
    fetch(`/api/overview/${range}`)
        .then(response => response.json())
        .then(data => {
            // 更新主要数值
            document.getElementById('today-checkin').textContent = data.today_checkin;
            document.getElementById('today-late').textContent = data.today_late;
            document.getElementById('total-employees').textContent = data.total_employees;
            document.getElementById('late-rate').textContent = data.late_rate;
            
            // 更新比较信息
            function updateCompareElement(id, trend, value) {
                const element = document.getElementById(id);
                if (!element) return;
                
                // 移除所有比较类
                element.classList.remove('compare-up', 'compare-down', 'compare-flat');
                
                // 设置箭头和类
                let arrow = '';
                if (trend === 'up') {
                    arrow = '↑';
                    element.classList.add('compare-up');
                } else if (trend === 'down') {
                    arrow = '↓';
                    element.classList.add('compare-down');
                } else {
                    arrow = '→';
                    element.classList.add('compare-flat');
                }
                
                // 设置显示文本
                const labels = {
                    'today': '较昨日',
                    'week': '较上周',
                    'month': '较上月',
                    'quarter': '较上季'
                };
                
                element.textContent = `${labels[range]} ${arrow} ${value}`;
            }
            
            // 更新各个比较元素
            updateCompareElement('today-checkin-compare', data.checkin_compare.trend, data.checkin_compare.value);
            updateCompareElement('today-late-compare', data.late_compare.trend, data.late_compare.value);
            updateCompareElement('total-employees-compare', data.total_compare.trend, data.total_compare.value);
            updateCompareElement('late-rate-compare', data.rate_compare.trend, data.rate_compare.value);
        })
        .catch(error => console.error('获取概览数据失败:', error));
}

// 获取趋势图数据
function fetchTrendData(range) {
    fetch(`/api/trend/${range}`)
        .then(response => response.json())
        .then(data => {
            trendChart.setOption(getTrendOption(data));
        })
        .catch(error => console.error('获取趋势数据失败:', error));
}

// 获取分布图数据
function fetchDistributionData(range) {
    fetch(`/api/late_distribution/${range}`)
        .then(response => response.json())
        .then(data => {
            distributionChart.setOption(getDistributionOption(data));
        })
        .catch(error => console.error('获取分布数据失败:', error));
}

// 获取签到批次分布数据
function fetchBatchData() {
    fetch('/api/batch_distribution')
        .then(response => response.json())
        .then(data => {
            batchChart.setOption(getBatchOption(data));
        })
        .catch(error => console.error('获取批次数据失败:', error));
}

// 获取加班数据
function fetchOvertimeData(range) {
    fetch(`/api/overtime/${range}`)
        .then(response => response.json())
        .then(data => {
            overtimeChart.setOption(getOvertimeOption(data));
        })
        .catch(error => console.error('获取加班数据失败:', error));
}

// 趋势图配置
function getTrendOption(data) {
    const xData = data.map(item => item.time || item.date || item.week || item.month);
    const yData = data.map(item => item.checkin);
    
    return {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'shadow'
            }
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: xData,
            axisLine: {
                lineStyle: {
                    color: '#B0C4DE'
                }
            }
        },
        yAxis: {
            type: 'value',
            axisLine: {
                lineStyle: {
                    color: '#B0C4DE'
                }
            },
            splitLine: {
                lineStyle: {
                    color: '#f0f0f0'
                }
            }
        },
        series: [{
            name: '签到人数',
            type: 'line',
            data: yData,
            smooth: true,
            lineStyle: {
                width: 3,
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                    { offset: 0, color: '#1E90FF' },
                    { offset: 1, color: '#4169E1' }
                ])
            },
            itemStyle: {
                color: '#1E90FF',
                borderRadius: 4
            },
            areaStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                    { offset: 0, color: 'rgba(30, 144, 255, 0.3)' },
                    { offset: 1, color: 'rgba(65, 105, 225, 0.1)' }
                ])
            }
        }]
    };
}

// 分布图配置
function getDistributionOption(data) {
    const isToday = currentRange === 'today';
    const xData = data.map(item => isToday ? item.time : item.day);
    const yData = data.map(item => isToday ? item.delay : item.late_count);
    
    // 处理数据格式
    const seriesData = data.map((item, index) => [xData[index], yData[index]]);
    
    return {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross'
            }
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: isToday ? 'category' : 'value',
            data: isToday ? xData : null,
            name: isToday ? '时间' : '天数',
            axisLine: {
                lineStyle: {
                    color: '#B0C4DE'
                }
            },
            splitLine: {
                lineStyle: {
                    color: '#f0f0f0'
                }
            }
        },
        yAxis: {
            type: 'value',
            name: isToday ? '迟到分钟' : '迟到人数',
            axisLine: {
                lineStyle: {
                    color: '#B0C4DE'
                }
            },
            splitLine: {
                lineStyle: {
                    color: '#f0f0f0'
                }
            }
        },
        series: [{
            name: isToday ? '迟到分布' : '每日迟到数',
            type: 'scatter',
            data: seriesData,
            symbolSize: 15,
            itemStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                    { offset: 0, color: '#4169E1' },
                    { offset: 1, color: '#1E90FF' }
                ])
            },
            emphasis: {
                itemStyle: {
                    shadowBlur: 10,
                    shadowColor: 'rgba(30, 144, 255, 0.5)'
                }
            }
        }]
    };
}

// 签到批次环形图配置
function getBatchOption(data) {
    return {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'item',
            formatter: '{a} <br/>{b}: {c} ({d}%)'
        },
        legend: {
            orient: 'vertical',
            left: 10,
            textStyle: {
                color: '#666'
            }
        },
        series: [{
            name: '签到批次',
            type: 'pie',
            radius: ['40%', '70%'],
            avoidLabelOverlap: false,
            itemStyle: {
                borderRadius: 10,
                borderColor: '#fff',
                borderWidth: 2
            },
            label: {
                show: false,
                position: 'center'
            },
            emphasis: {
                label: {
                    show: true,
                    fontSize: 16,
                    fontWeight: 'bold'
                }
            },
            labelLine: {
                show: false
            },
            data: data
        }]
    };
}

// 加班图配置
function getOvertimeOption(data) {
    const xData = data.map(item => item.department);
    const yData = data.map(item => item.overtime);
    
    return {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'shadow'
            }
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: xData,
            axisLine: {
                lineStyle: {
                    color: '#B0C4DE'
                }
            }
        },
        yAxis: {
            type: 'value',
            name: '加班小时',
            axisLine: {
                lineStyle: {
                    color: '#B0C4DE'
                }
            },
            splitLine: {
                lineStyle: {
                    color: '#f0f0f0'
                }
            }
        },
        series: [{
            name: '加班时长',
            type: 'bar',
            data: yData,
            barWidth: '60%',
            itemStyle: {
                color: function(params) {
                    const colorList = [
                        '#1E90FF', '#4169E1', '#0056D2', '#6495ED', '#87CEFA'
                    ];
                    return colorList[params.dataIndex % colorList.length];
                },
                borderRadius: [4, 4, 0, 0]
            }
        }]
    };
}

// 切换时间范围
function switchTimeRange(range) {
    currentRange = range;
    // 更新按钮状态
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.range === range);
    });
    // 更新图表数据
    fetchTrendData(range);
    fetchDistributionData(range);
    fetchOvertimeData(range);
    fetchOverview(range);
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    // 初始化图表
    initCharts();
    
    // 获取初始数据
    fetchOverview(currentRange);
    fetchTrendData(currentRange);
    fetchDistributionData(currentRange);
    fetchOvertimeData(currentRange);
    fetchBatchData();

    
    // 绑定时间筛选按钮事件
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            switchTimeRange(this.dataset.range);
        });
    });
    
    // 窗口大小变化时重绘图表
    window.addEventListener('resize', function() {
        trendChart.resize();
        distributionChart.resize();
        overtimeChart.resize();
        batchChart.resize();

    });
});