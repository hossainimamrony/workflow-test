from django.contrib import admin
from django.urls import include, path
from django.views.generic import RedirectView
from apps.carsale_scraper.web import views as carsale_web_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("favicon.ico", carsale_web_views.favicon),
    path("control-panel-sw.js", carsale_web_views.control_panel_sw),
    path("", RedirectView.as_view(pattern_name="common:dashboard", permanent=False)),
    path("api/", include("apps.carsale_scraper.api_root_urls")),
    path("dashboard/", include("apps.common.urls")),
    path("workflows/global-make-model-video/", include("apps.global_make_model_video.urls")),
    path("workflows/real-footage-reels/", include("apps.real_footage_reels.urls")),
    path("workflows/car-comparison/", include("apps.carsale_scraper.urls")),
]
