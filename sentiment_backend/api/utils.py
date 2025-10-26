import re
import pickle
import numpy as np
import tensorflow as tf
from loguru import logger
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
from preprocess.utils import segment_text, load_claim_words, load_optimized_stopwords
from sentiment_backend.settings import MODEL_PATH, TOKENIZER_PATH

# --------------------------
# 连词情感修正（提升中评识别率）
# --------------------------
def correct_by_conjunction(comment: str) -> float:
    positive_conj = {"而且", "并且", "此外"}
    negative_conj = {"但是", "然而", "不过", "却"}
    pos_count = sum(1 for conj in positive_conj if conj in comment)
    neg_count = sum(1 for conj in negative_conj if conj in comment)
    return (pos_count - neg_count) / max(1, pos_count + neg_count)

# --------------------------
# 加载模型和Tokenizer（启动时只加载一次）
# --------------------------
def load_pretrained_resources():
    try:
        # 加载模型
        model = load_model(MODEL_PATH)
        logger.success("模型加载成功！")
        
        # 加载Tokenizer
        with open(TOKENIZER_PATH, "rb") as f:
            tokenizer = pickle.load(f)
        logger.success("Tokenizer加载成功！")
        
        # 加载预处理资源
        claim_words = load_claim_words()
        stopwords, _ = load_optimized_stopwords(claim_words)
        return model, tokenizer, stopwords, claim_words
    except Exception as e:
        logger.error(f"加载失败：{str(e)}")
        raise

# --------------------------
# 单条影评预测（核心功能）
# --------------------------
def predict_sentiment(comment: str, model, tokenizer, stopwords, claim_words):
    # 1. 清洗文本
    comment_clean = re.sub(r"[^\u4e00-\u9fa5\s]", "", comment.strip())
    if len(comment_clean) < 5:
        return {"error": "评论太短啦，至少5个中文字符！"}
    
    # 2. 分词
    seg_text = segment_text(comment_clean, stopwords, claim_words, NEGATION_WORDS)
    if not seg_text:
        return {"error": "没识别到有效词语，换个影评试试！"}
    seg_str = " ".join(seg_text)
    
    # 3. 转换为模型能识别的格式
    sequence = tokenizer.texts_to_sequences([seg_str])
    X = pad_sequences(sequence, maxlen=157, padding="post")
    
    # 4. 预测情感
    pred_proba = model.predict(X, verbose=0)[0]
    pred_label = np.argmax(pred_proba)
    
    # 5. 优化中评识别
    corr_score = correct_by_conjunction(comment_clean)
    if abs(corr_score) < 0.3:  # 中性连词，提升中评概率
        pred_proba[1] = min(1.0, pred_proba[1] + 0.12)
        pred_proba[0] = max(0.0, pred_proba[0] - 0.06)
        pred_proba[2] = max(0.0, pred_proba[2] - 0.08)
    # 新增：基于差评特征词的概率补偿
# 定义常见差评特征词（可根据你的数据扩充）
    negative_features = [
    "差", "烂", "糟糕", "失望", "浪费", "无聊", "尴尬", "不推荐", 
    "生硬", "拖沓", "敷衍", "牵强", "乏味", "难看", "垃圾", 
    "不值", "无趣", "混乱", "糟糕透顶", "一无是处","两星半","两星"
]

# 统计评论中出现的差评特征词数量
    negative_count = sum(1 for word in negative_features if word in comment_clean)

# 根据数量调整差评概率
    if negative_count >= 2:
    # 2个及以上差评词，明显提升差评概率
        pred_proba[0] = min(1.0, pred_proba[0] + 0.15)  # 差评概率+15%
        pred_proba[1] = max(0.0, pred_proba[1] - 0.08)   # 中评-8%
        pred_proba[2] = max(0.0, pred_proba[2] - 0.07)   # 好评-7%
    elif negative_count == 1:
    # 1个差评词，小幅提升
        pred_proba[0] = min(1.0, pred_proba[0] + 0.08)  # 差评+8%
        pred_proba[2] = max(0.0, pred_proba[2] - 0.08)   # 好评-8%
    
    
    # 6. 返回结果
    sentiment_map = {0: "差评", 1: "中评", 2: "好评"}
    final_label = np.argmax(pred_proba)
    return {
        "你输入的影评": comment_clean,
        "预测结果": sentiment_map[final_label],
        "置信度": f"{round(pred_proba[final_label]*100, 2)}%",
        "三类概率": {
            "差评": f"{round(pred_proba[0]*100, 2)}%",
            "中评": f"{round(pred_proba[1]*100, 2)}%",
            "好评": f"{round(pred_proba[2]*100, 2)}%"
        }
    }

# --------------------------
# 初始化资源（全局只用一次）
# --------------------------
model, tokenizer, stopwords, claim_words = load_pretrained_resources()
# 补充否定词（从preprocess.utils导入）
from preprocess.utils import NEGATION_WORDS