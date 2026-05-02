from django.urls import path
from .web import views as web_views

urlpatterns = [
    path("config", web_views.api_config),
    path("status", web_views.api_status),
    path("version", web_views.api_version),
    path("progress/stream", web_views.api_progress_stream),
    path("files", web_views.api_files),
    path("identified", web_views.api_identified),
    path("comparisons", web_views.api_comparisons),
    path("carbarn-inventory/refresh", web_views.api_carbarn_inventory_refresh),
    path("not-found", web_views.api_not_found),
    path("image", web_views.api_image),
    path("manual/urls", web_views.api_manual_urls),
    path("run", web_views.api_run),
    path("stop", web_views.api_stop),
    path("open-output", web_views.api_open_output),
    path("session/mode", web_views.api_session_mode),
    path("open-antibot-url", web_views.api_open_antibot_url),
    path("manual/refresh-one", web_views.api_manual_refresh_one),
]
