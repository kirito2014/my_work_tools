// script.js
const cards = document.querySelectorAll('.card');
const cardInners = document.querySelectorAll('.card-inner');

const localQuotes = [
    { content: "人生若只如初见，何事秋风悲画扇。", from: "纳兰容若" },
    { content: "曾经沧海难为水，除却巫山不是云。", from: "元稹" },
    { content: "衣带渐宽终不悔，为伊消得人憔悴。", from: "柳永" },
    { content: "众里寻他千百度，蓦然回首，那人却在灯火阑珊处。", from: "辛弃疾" },
    { content: "此情可待成追忆，只是当时已惘然。", from: "李商隐" },
    { content: "红豆生南国，春来发几枝。愿君多采撷，此物最相思。", from: "王维" },
    { content: "春风得意马蹄疾，一日看尽长安花。", from: "孟郊" },
    { content: "醉后不知天在水，满船清梦压星河。", from: "唐温如" }
];

// 获取卡片文字内容
async function fetchCardTexts() {
    try {
        console.log('开始获取卡片文字...');
        
        // 随机获取4条不重复的句子
        const shuffled = [...localQuotes].sort(() => 0.5 - Math.random());
        const selected = shuffled.slice(0, 4);
        
        const cardTextsData = selected.map(quote => ({
            title: quote.content.substring(0, 4),
            content: quote.content,
            from: quote.from
        }));
        
        console.log('获取到的数据:', cardTextsData);
        
        // 更新卡片内容
        cardTextsData.forEach((cardText, index) => {
            const card = cards[index];
            if (card) {
                const content = card.querySelector('.card-back .content');
                if (content) {
                    const title = content.querySelector('.card-title');
                    const text = content.querySelector('.card-text');
                    if (title) title.textContent = cardText.title;
                    if (text) text.textContent = `${cardText.content} —— ${cardText.from}`;
                }
            }
        });

    } catch (error) {
        console.error('获取卡片文字失败:', error);
        // 显示错误信息在卡片上
        cards.forEach(card => {
            const content = card.querySelector('.card-back .content');
            if (content) {
                const title = content.querySelector('.card-title');
                const text = content.querySelector('.card-text');
                if (title) title.textContent = '加载失败';
                if (text) text.textContent = '无法获取内容，请刷新页面重试';
            }
        });
    }
}

// 添加动画状态管理
const AnimationState = {
    RANDOM_FLOAT: 'random_float',
    HOVER: 'hover',
    TRANSITION: 'transition',
    FLIP: 'flip'
};

// 存储卡片当前位置
function getCurrentTransform(cardInner) {
    const style = window.getComputedStyle(cardInner);
    const matrix = new DOMMatrix(style.transform);
    return {
        x: matrix.m41,
        y: matrix.m42,
        rotate: Math.atan2(matrix.m21, matrix.m11) * (180 / Math.PI)
    };
}

// 生成新的随机位置
function generateRandomPosition() {
    return {
        x: (Math.random() - 0.5) * 10,
        y: (Math.random() - 0.5) * 10,
        rotate: (Math.random() - 0.5) * 4,
        rotateX: (Math.random() - 0.5) * 5,
        rotateY: (Math.random() - 0.5) * 5,
        tiltX: (Math.random() - 0.5) * 3,
        tiltY: (Math.random() - 0.5) * 3
    };
}

// 平滑过渡到新位置
function smoothTransition(cardInner, startPos, targetPos, duration = 2000) {
    const startTime = performance.now();
    const isFlipped = cardInner.parentElement.classList.contains('flipped');
    const baseRotation = isFlipped ? 180 : 0;
    
    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);
        
        const easeProgress = progress < .5 ? 
            4 * progress * progress * progress : 
            1 - Math.pow(-2 * progress + 2, 3) / 2;
        
        const x = startPos.x + (targetPos.x - startPos.x) * easeProgress;
        const y = startPos.y + (targetPos.y - startPos.y) * easeProgress;
        const rotate = startPos.rotate + (targetPos.rotate - startPos.rotate) * easeProgress;
        const rotateX = (startPos.rotateX || 0) + ((targetPos.rotateX || 0) - (startPos.rotateX || 0)) * easeProgress;
        const rotateY = (startPos.rotateY || 0) + ((targetPos.rotateY || 0) - (startPos.rotateY || 0)) * easeProgress;
        const tiltX = (startPos.tiltX || 0) + ((targetPos.tiltX || 0) - (startPos.tiltX || 0)) * easeProgress;
        const tiltY = (startPos.tiltY || 0) + ((targetPos.tiltY || 0) - (startPos.tiltY || 0)) * easeProgress;
        
        // 应用变换时保持翻转状态
        cardInner.style.transform = `
            translate(${x}px, ${y}px)
            rotateY(${baseRotation + rotateY + tiltY}deg)
            rotateX(${rotateX + tiltX}deg)
            rotate(${rotate}deg)
        `;

        // 更新图片位置
        const frontImg = cardInner.querySelector('.card-front .img');
        const backImg = cardInner.querySelector('.card-back .img');
        const imgTransform = `
            translate3d(${-x * 0.5}px, ${-y * 0.5}px, 20px)
            rotateX(${-(rotateX + tiltX) * 0.5}deg)
            rotateY(${-(rotateY + tiltY) * 0.5}deg)
            rotate(${-rotate * 0.5}deg)
            scale(1.1)
        `;
        
        if (frontImg) frontImg.style.transform = imgTransform;
        if (backImg) backImg.style.transform = imgTransform;
        
        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }
    
    requestAnimationFrame(update);
}

// 修改卡片事件处理
function initializeCardEvents() {
    cards.forEach((card, index) => {
        const cardInner = cardInners[index];
        let currentState = AnimationState.RANDOM_FLOAT;
        let currentPosition = { 
            x: 0, 
            y: 0, 
            rotate: 0,
            rotateX: 0,
            rotateY: 0,
            tiltX: 0,
            tiltY: 0
        };
        
        // 随机浮动动画
        function startRandomFloat() {
            if (currentState !== AnimationState.RANDOM_FLOAT) return;
            
            const targetPos = generateRandomPosition();
            smoothTransition(cardInner, currentPosition, targetPos);
            currentPosition = targetPos;
            
            setTimeout(() => {
                if (currentState === AnimationState.RANDOM_FLOAT) {
                    startRandomFloat();
                }
            }, 2000);
        }
        
        // 鼠标进入
        card.addEventListener('mouseenter', () => {
            if (currentState === AnimationState.FLIP) return;
            currentState = AnimationState.HOVER;
            currentPosition = getCurrentTransform(cardInner);
            cardInner.style.transition = 'transform 0.3s ease';
        });
        
        // 鼠标移动
        card.addEventListener('mousemove', (e) => {
            if (currentState !== AnimationState.HOVER) return;
            
            const rect = card.getBoundingClientRect();
            const x = (e.clientX - rect.left - rect.width / 2) / rect.width;
            const y = (e.clientY - rect.top - rect.height / 2) / rect.height;
            
            const isFlipped = card.classList.contains('flipped');
            const baseRotation = isFlipped ? 180 : 0;
            
            // 添加图片透视效果
            const frontImg = card.querySelector('.card-front .img');
            const backImg = card.querySelector('.card-back .img');
            const imgTransform = `
                translate3d(${x * -40}px, ${y * -40}px, 20px)
                scale(1.1)
            `;
            
            // 平滑过渡图变换
            if (frontImg) {
                frontImg.style.transition = 'transform 0.3s ease-out';
                frontImg.style.transform = imgTransform;
            }
            if (backImg) {
                backImg.style.transition = 'transform 0.3s ease-out';
                backImg.style.transform = imgTransform;
            }
            
            // 应用卡片旋转
            cardInner.style.transform = `
                rotateY(${baseRotation + x * 20}deg)
                rotateX(${-y * 20}deg)
            `;
        });
        
        // 鼠标离开
        card.addEventListener('mouseleave', () => {
            if (currentState === AnimationState.FLIP) return;
            currentState = AnimationState.TRANSITION;
            
            const frontImg = card.querySelector('.card-front .img');
            const backImg = card.querySelector('.card-back .img');
            const resetTransform = 'translate3d(0, 0, 0) scale(1.15)';
            
            // 平滑重置图片位置
            if (frontImg) {
                frontImg.style.transition = 'transform 0.5s ease';
                frontImg.style.transform = resetTransform;
            }
            if (backImg) {
                backImg.style.transition = 'transform 0.5s ease';
                backImg.style.transform = resetTransform;
            }
            
            const isFlipped = card.classList.contains('flipped');
            cardInner.style.transition = 'transform 0.5s ease';
            cardInner.style.transform = isFlipped ? 
                'rotateY(180deg)' : 'translate(0, 0) rotateX(0deg) rotateY(0deg) rotate(0deg)';
            
            setTimeout(() => {
                if (currentState === AnimationState.TRANSITION) {
                    currentState = AnimationState.RANDOM_FLOAT;
                    currentPosition = { 
                        x: 0, y: 0, rotate: 0, 
                        rotateX: 0, rotateY: 0, 
                        tiltX: 0, tiltY: 0 
                    };
                    startRandomFloat();
                }
            }, 500);
        });
        
        // 点击翻转
        card.addEventListener('click', () => {
            currentState = AnimationState.FLIP;
            cardInner.style.transition = 'transform 0.5s ease';
            card.classList.toggle('flipped');
            
            setTimeout(() => {
                const isFlipped = card.classList.contains('flipped');
                const baseRotation = isFlipped ? 180 : 0;
                
                // 设置基础翻转状态
                cardInner.style.transform = `rotateY(${baseRotation}deg)`;
                
                currentPosition = { 
                    x: 0, y: 0, rotate: 0, 
                    rotateX: 0, rotateY: 0, 
                    tiltX: 0, tiltY: 0 
                };
                
                currentState = card.matches(':hover') ? 
                    AnimationState.HOVER : AnimationState.RANDOM_FLOAT;
                
                // 移除 isFlipped 检查，让翻转后的卡片也能随机浮动
                if (currentState === AnimationState.RANDOM_FLOAT) {
                    startRandomFloat();
                }
            }, 500);
        });
        
        // 开始初始动画
        startRandomFloat();
    });
}

// 获取底部一言
function fetchHitokoto() {
    fetch('https://v1.hitokoto.cn')
        .then(response => response.json())
        .then(data => {
            const hitokoto = document.querySelector('#hitokoto_text');
            hitokoto.href = `https://hitokoto.cn/?uuid=${data.uuid}`;
            hitokoto.innerText = `${data.hitokoto} —— ${data.from}`;
        })
        .catch(console.error);
}

// 添加主题切换功能
function initThemeSwitch() {
    const themeToggle = document.getElementById('themeToggle');
    const html = document.documentElement;
    
    // 从本地存储加载主题
    const savedTheme = localStorage.getItem('theme') || 'dark';
    html.setAttribute('data-theme', savedTheme);

    themeToggle.addEventListener('click', () => {
        const currentTheme = html.getAttribute('data-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        
        html.setAttribute('data-theme', newTheme);
        localStorage.setItem('theme', newTheme);
    });
}

// 修改初始化函数
async function initialize() {
    initThemeSwitch();
    await fetchCardTexts();
    initializeCardEvents();
    fetchHitokoto();
    setInterval(fetchHitokoto, 30000);
}

// 启动应用
initialize();