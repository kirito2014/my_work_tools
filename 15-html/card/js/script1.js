// script.js
//偏移改变
const card = document.querySelector('.card');

card.addEventListener('mousemove', (e) => {
    const cardRect = card.getBoundingClientRect(); // 获取卡片元素的边界矩形
    const cardWidth = cardRect.width; // 获取卡片宽度
    const cardHeight = cardRect.height; //  获取卡片高度
    const cardLeft = cardRect.left; // 获取卡片左边距
    const cardTop = cardRect.top; //  获取卡片上边距
    const mouseX = e.clientX - cardLeft - cardWidth / 2;
    const mouseY = e.clientY - cardTop - cardHeight / 2;
    const px = mouseX / cardWidth; // 鼠标在卡片上的X坐标
    const py = mouseY / cardHeight; // 鼠标在卡片上的Y坐标
    setTransform(card, px, py); // 传入鼠标移动的元素
  });
// 监听鼠标离开事件
card.addEventListener('mouseleave', () => {
    card.style.transform = `rotateX(0deg) rotateY(0deg)`;
    const img = card.querySelector('.img');
    if (img) {
      img.style.transform = `translateX(0px) translateY(0px)`;
    }
  });
  // 设置偏移
function setTransform(target, px, py) {
if (!target) {
    return;
}
const imgs = target.getElementsByClassName('img');
const img = imgs[0]; // 获取图片
if (!img) {
    return;
}
img.style.setProperty('transform',`translateX(${px * -40}px) translateY(${py * -40}px)`);
target.style.setProperty('transform',`rotateX(${py * 30}deg) rotateY(${px * 30}deg)`);

}