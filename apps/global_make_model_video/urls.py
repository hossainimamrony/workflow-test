from django.urls import path
from .views import WorkflowHomeView

app_name = "global_make_model_video"

urlpatterns = [
    path("", WorkflowHomeView.as_view(), name="home"),
]
