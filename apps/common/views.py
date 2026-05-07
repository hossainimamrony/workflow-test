from django.views.generic import TemplateView


class DashboardView(TemplateView):
    template_name = "common/dashboard.html"


class ApiDocsView(TemplateView):
    template_name = "common/api_docs.html"
