from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .utils import predict_sentiment, model, tokenizer, stopwords, claim_words

class SentimentPredictView(APIView):
    """接收影评，返回情感结果"""
    def post(self, request):
        # 1. 获取用户输入的影评
        comment = request.data.get("comment", "").strip()
        if not comment:
            return Response({"error": "请输入影评内容！"}, status=400)
        
        # 2. 调用预测函数
        result = predict_sentiment(comment, model, tokenizer, stopwords, claim_words)
        
        # 3. 返回结果
        if "error" in result:
            return Response(result, status=400)
        return Response(result, status=200)

class ModelInfoView(APIView):
    """返回模型信息（测试用）"""
    def get(self, request):
        return Response({
            "模型版本": "初学者测试版",
            "功能": "影评情感分类（差评/中评/好评）",
            "测试准确率": "约82%"
        })