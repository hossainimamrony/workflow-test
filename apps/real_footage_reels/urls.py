from django.urls import path

from .views import (
    JobControlApiView,
    JobsApiView,
    MetaApiView,
    RunDeleteApiView,
    RunDetailApiView,
    RunThumbnailApiView,
    RunsApiView,
    RunAssetApiView,
    VehicleInventoryRefreshApiView,
    VehicleInventorySearchApiView,
    VehicleInventoryStatusApiView,
    WorkflowHomeView,
)

app_name = "real_footage_reels"

urlpatterns = [
    path("", WorkflowHomeView.as_view(), name="home"),
    path("workflow", WorkflowHomeView.as_view(), name="workflow"),
    path("activity", WorkflowHomeView.as_view(), name="activity"),
    path("runs", WorkflowHomeView.as_view(), name="runs"),
    path("runs/<str:run_id>", WorkflowHomeView.as_view(), name="run-page"),
    path("api/meta", MetaApiView.as_view(), name="api-meta"),
    path("api/jobs", JobsApiView.as_view(), name="api-jobs"),
    path("api/jobs/<str:job_id>/control", JobControlApiView.as_view(), name="api-job-control"),
    path("api/runs", RunsApiView.as_view(), name="api-runs"),
    path("api/runs/<str:run_id>", RunDetailApiView.as_view(), name="api-run-detail"),
    path("api/runs/<str:run_id>/thumbnail", RunThumbnailApiView.as_view(), name="api-run-thumbnail"),
    path("api/runs/<str:run_id>/asset", RunAssetApiView.as_view(), name="api-run-asset"),
    path("api/runs/<str:run_id>/prepare", JobsApiView.as_view(), name="api-run-prepare"),
    path("api/runs/<str:run_id>/identify", JobsApiView.as_view(), name="api-run-identify"),
    path("api/runs/<str:run_id>/compose", JobsApiView.as_view(), name="api-run-compose"),
    path("api/runs/<str:run_id>/end-scene", JobsApiView.as_view(), name="api-run-end-scene"),
    path("api/runs/<str:run_id>/voiceover/draft", JobsApiView.as_view(), name="api-run-voiceover-draft"),
    path("api/runs/<str:run_id>/voiceover/apply", JobsApiView.as_view(), name="api-run-voiceover-apply"),
    path("api/runs/<str:run_id>/delete", RunDeleteApiView.as_view(), name="api-run-delete"),
    path("api/vehicle-inventory/status", VehicleInventoryStatusApiView.as_view(), name="api-vehicle-status"),
    path("api/vehicle-inventory/search", VehicleInventorySearchApiView.as_view(), name="api-vehicle-search"),
    path("api/vehicle-inventory/refresh", VehicleInventoryRefreshApiView.as_view(), name="api-vehicle-refresh"),
]
