// script.js
const card = document.querySelector('.card'); // 获取卡片元素

// 监听鼠标移动事件
card.addEventListener('mousemove', (e) => {
  const cardRect = card.getBoundingClientRect(); // 获取卡片元素的边界矩形
  const cardWidth = cardRect.width; // 获取卡片宽度
  const cardHeight = cardRect.height; //  获取卡片高度
  const mouseX = e.clientX - cardRect.left - cardWidth / 2; // 获取鼠标相对于卡片元素的X坐标
  const mouseY = e.clientY - cardRect.top - cardHeight / 2; // 获取鼠标相对于卡片元素的Y坐标
  const px = mouseX / cardWidth; // 计算鼠标相对于卡片元素的X偏移
  const py = mouseY / cardHeight; // 计算鼠标相对于卡片元素的Y偏移

  setTransform(card, px, py); // 设置卡片元素的偏移效果
});

// 监听鼠标离开事件
card.addEventListener('mouseleave', () => {
  card.style.transform = `rotateX(0deg) rotateY(0deg)`;
  const img = card.querySelector('.img');
  if (img) {
    img.style.transform = `translateX(0px) translateY(0px)`;
  }
});

// 设置偏移效果
function setTransform(element, px, py) {
  const img = element.querySelector('.img'); // 获取卡片中的图片元素
  if (img) {
    img.style.transform = `translateX(${px * -40}px) translateY(${py * -40}px)`; // 设置图片元素的偏移效果
  }
  element.style.transform = `rotateX(${py * 20}deg) rotateY(${px * 20}deg)`; 
}