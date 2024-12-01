const CACHE_KEY = 'cardImages';
const CACHE_DURATION = 12 * 60 * 60 * 1000; // 12小时的毫秒数
const UNSPLASH_ACCESS_KEY = 'l86CVcZ8URO8DiQMXHjaMsCfjd0H3xCIj2zqU27oXDI';

// 检查缓存
function getValidCachedImages() {
    const cached = localStorage.getItem(CACHE_KEY);
    if (!cached) return null;

    const { timestamp, images } = JSON.parse(cached);
    const now = new Date().getTime();
    
    if (now - timestamp > CACHE_DURATION) {
        localStorage.removeItem(CACHE_KEY);
        return null;
    }
    
    return images;
}

// 保存到缓存
function cacheImages(images) {
    const cacheData = {
        timestamp: new Date().getTime(),
        images: images
    };
    localStorage.setItem(CACHE_KEY, JSON.stringify(cacheData));
}

// 获取 Unsplash 图片
async function fetchUnsplashImages() {
    try {
        const response = await fetch(
            'https://api.unsplash.com/photos/random?count=4&query=nature&orientation=landscape', 
            {
                headers: {
                    'Authorization': `Client-ID ${UNSPLASH_ACCESS_KEY}`
                }
            }
        );
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        return data.map(photo => ({
            url: photo.urls.regular,
            photographer: photo.user.name,
            photographerUrl: `https://unsplash.com/@${photo.user.username}`
        }));
    } catch (error) {
        console.error('Error fetching Unsplash images:', error);
        return null;
    }
}

// 获取图片（优先使用缓存）
async function getImages() {
    // 先检查缓存
    const cachedImages = getValidCachedImages();
    if (cachedImages) {
        return cachedImages;
    }

    // 如果没有缓存或缓存已过期，获取新图片
    const images = await fetchUnsplashImages();
    if (images) {
        cacheImages(images);
        return images;
    }

    // 如果获取失败，返回默认图片
    return Array(4).fill({
        url: 'https://images.unsplash.com/photo-1732525830182-b0e1a93f3679?q=80&w=3087&auto=format&fit=crop',
        photographer: 'Default',
        photographerUrl: '#'
    });
}

export { getImages }; 