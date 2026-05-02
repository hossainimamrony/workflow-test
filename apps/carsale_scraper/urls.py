from django.urls import path
from .web import views as web_views

app_name = "carsale_scraper"

urlpatterns = [
    path("", web_views.dashboard, name="home"),
    path("api/config/", web_views.api_config, name="api-config"),
    path("api/status/", web_views.api_status, name="api-status"),
    path("api/version/", web_views.api_version, name="api-version"),
    path("api/progress/stream/", web_views.api_progress_stream, name="api-progress-stream"),
    path("api/files/", web_views.api_files, name="api-files"),
    path("api/identified/", web_views.api_identified, name="api-identified"),
    path("api/comparisons/", web_views.api_comparisons, name="api-comparisons"),
    path("api/carbarn-inventory/refresh/", web_views.api_carbarn_inventory_refresh, name="api-carbarn-inventory-refresh"),
    path("api/not-found/", web_views.api_not_found, name="api-not-found"),
    path("api/image/", web_views.api_image, name="api-image"),
    path("api/manual/urls/", web_views.api_manual_urls, name="api-manual-urls"),
    path("api/run/", web_views.api_run, name="api-run"),
    path("api/stop/", web_views.api_stop, name="api-stop"),
    path("api/open-output/", web_views.api_open_output, name="api-open-output"),
    path("api/session/mode/", web_views.api_session_mode, name="api-session-mode"),
    path("api/open-antibot-url/", web_views.api_open_antibot_url, name="api-open-antibot-url"),
    path("api/manual/refresh-one/", web_views.api_manual_refresh_one, name="api-manual-refresh-one"),
    path("favicon.ico", web_views.favicon, name="favicon"),
    path("control-panel-sw.js", web_views.control_panel_sw, name="control-panel-sw"),
]
