from django.urls import path
from .views import ApiDocsView, DashboardView

app_name = "common"

urlpatterns = [
    path("", DashboardView.as_view(), name="dashboard"),
    path("api-docs/", ApiDocsView.as_view(), name="api-docs"),
]
