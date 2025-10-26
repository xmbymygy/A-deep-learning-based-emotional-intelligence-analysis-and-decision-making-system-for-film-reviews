import sys
import os
import re
import jieba
import numpy as np
import tensorflow as tf
from tensorflow.keras.preprocessing.sequence import pad_sequences

# --------------------------
# 路径配置
# --------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))  # model_train目录
parent_dir = os.path.dirname(current_dir)  # 模块二 数据 预处理目录
sys.path.append(parent_dir)

# 模型和分词器路径（确保与训练时保存路径一致）
MODEL_PATH = f"{current_dir}\\sentiment_lstm.h5"
TOKENIZER_PATH = f"{parent_dir}\\tokenizer.pkl"  # 后面会生成

# 预处理参数（与训练时保持一致）
VOCAB_SIZE = 10000
MAX_SEQ_LEN = 157

# 情感标签映射
LABEL_MAP = {0: "差评", 1: "中评", 2: "好评"}


# --------------------------
# 文本预处理函数（与数据预处理.py保持一致）
# --------------------------
def clean_text(text):
    """清洗文本：保留中文、英文、数字和常见标点"""
    if not isinstance(text, str):
        return ""
    # 过滤特殊符号，保留基本字符
    text = re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9,.!?;，。！？；]", " ", text)
    # 去除多余空格
    text = re.sub(r"\s+", " ", text).strip()
    return text


def segment_text(text, stopwords):
    """分词并过滤停用词"""
    words = jieba.lcut(text)
    # 过滤停用词和长度为1的词（除否定词）
    negation_words = {"不", "没", "无", "未", "非", "别", "莫"}
    filtered = [
        word for word in words
        if (word not in stopwords) or (word in negation_words)
        and (len(word) > 1 or word in negation_words)
    ]
    return filtered


def load_stopwords():
    """加载停用词（从数据预处理目录读取）"""
    stopwords = set()
    # 读取hit_stopwords和baidu_stopwords
    stopword_files = [
        f"{parent_dir}\\hit_stopwords.txt",
        f"{parent_dir}\\baidu_stopwords.txt"
    ]
    for file in stopword_files:
        try:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    stopwords.add(line.strip())
        except UnicodeDecodeError:
            with open(file, "r", encoding="gbk") as f:
                for line in f:
                    stopwords.add(line.strip())
    return stopwords


# --------------------------
# 预测主函数
# --------------------------
def predict_sentiment(text, model, tokenizer, stopwords):
    """
    预测单条影评的情感类别
    :param text: 输入的影评文本
    :param model: 加载的LSTM模型
    :param tokenizer: 训练时使用的Tokenizer
    :param stopwords: 停用词集合
    :return: 预测标签（0/1/2）和情感类别（差评/中评/好评）
    """
    # 1. 文本清洗
    cleaned_text = clean_text(text)
    if not cleaned_text:
        return None, "输入文本为空或清洗后无有效内容"
    
    # 2. 分词
    seg_words = segment_text(cleaned_text, stopwords)
    if not seg_words:
        return None, "分词后无有效词语"
    
    # 3. 转换为序列
    text_seq = " ".join(seg_words)
    sequence = tokenizer.texts_to_sequences([text_seq])
    
    # 4. 序列填充
    padded_sequence = pad_sequences(
        sequence, maxlen=MAX_SEQ_LEN, padding="post", truncating="post"
    )
    
    # 5. 模型预测
    pred_prob = model.predict(padded_sequence, verbose=0)[0]
    pred_label = np.argmax(pred_prob)
    confidence = round(pred_prob[pred_label] * 100, 2)  # 置信度（百分比）
    
    return pred_label, f"{LABEL_MAP[pred_label]}（置信度：{confidence}%）"


# --------------------------
# 运行入口：交互式输入测试
# --------------------------
if __name__ == "__main__":
    # 加载停用词
    stopwords = load_stopwords()
    print("停用词加载完成，共{}个".format(len(stopwords)))
    
    # 加载模型
    try:
        model = tf.keras.models.load_model(MODEL_PATH)
        print(f"模型加载成功：{MODEL_PATH}")
    except Exception as e:
        print(f"模型加载失败：{str(e)}")
        sys.exit(1)
    
    # 加载分词器（需要先保存，见下方说明）
    try:
        import pickle
        with open(TOKENIZER_PATH, "rb") as f:
            tokenizer = pickle.load(f)
        print(f"分词器加载成功：{TOKENIZER_PATH}")
    except Exception as e:
        print(f"分词器加载失败：{str(e)}")
        print("请先在数据预处理阶段保存分词器（见说明）")
        sys.exit(1)
    
    # 交互式测试
    print("\n===== 影评情感预测工具 =====")
    print("输入'q'退出测试")
    while True:
        text = input("\n请输入影评文本：")
        if text.lower() == "q":
            print("测试结束")
            break
        label, result = predict_sentiment(text, model, tokenizer, stopwords)
        print(f"预测结果：{result}")