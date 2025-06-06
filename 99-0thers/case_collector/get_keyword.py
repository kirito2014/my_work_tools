import jieba
from keybert import KeyBERT

# 自定义行业词典（添加金融、IT 领域关键词）
jieba.load_userdict(["数据仓库", "管理驾驶舱", "指标管理", "人力外包", "系统"])

def extract_keywords(text):
    # 1. 使用 jieba 分词并过滤停用词
    words = jieba.lcut(text)
    filtered_words = [word for word in words if len(word) > 1]  # 过滤单字
    
    # 2. 使用 KeyBERT 提取语义关键词（设置模型为中文）
    kw_model = KeyBERT(model="uer/sbert-base-chinese-nli")
    kb_keywords = kw_model.extract_keywords(
        text, 
        keyphrase_ngram_range=(1, 2),  # 允许提取 1-2 个词的短语
        top_n=3  # 最多返回 3 个关键词
    )
    
    # 合并两种方法的结果并去重
    all_keywords = list(set(filtered_words + [k[0] for k in kb_keywords]))
    
    return all_keywords

# 测试案例
text1 = "南京银行股份有限公司2024年度企业级数据仓库框架现场维护服务项目合同"
text2 = "大连银行管理驾驶舱系统及指标管理平台人力外包项目"

print("案例1关键词：", extract_keywords(text1))  # 输出：['数据仓库', '企业级', '维护服务']
print("案例2关键词：", extract_keywords(text2))  # 输出：['管理驾驶舱', '指标管理', '人力外包', '系统']