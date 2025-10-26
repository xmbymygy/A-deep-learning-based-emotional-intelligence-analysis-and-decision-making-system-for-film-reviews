from django.urls import path
from .views import SentimentPredictView, ModelInfoView

urlpatterns = [
    path("predict/", SentimentPredictView.as_view(), name="predict"),
    path("info/", ModelInfoView.as_view(), name="info"),
]