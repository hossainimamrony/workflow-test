from django.urls import path
from .web import views as web_views

urlpatterns = [
    path("comparisons", web_views.api_comparisons),
]
