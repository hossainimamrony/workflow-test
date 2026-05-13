from django.urls import path
from .web import views as web_views

app_name = "carsale_scraper"

urlpatterns = [
    path("", web_views.dashboard, name="home"),
    path("api/comparisons/", web_views.api_comparisons, name="api-comparisons"),
    path("api/my-car-list/add/", web_views.api_my_car_list_add, name="api-my-car-list-add"),
    path("favicon.ico", web_views.favicon, name="favicon"),
    path("control-panel-sw.js", web_views.control_panel_sw, name="control-panel-sw"),
]
