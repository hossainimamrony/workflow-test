from django.contrib import admin

from .models import ReelRenderJob, ReelRun


@admin.register(ReelRenderJob)
class ReelRenderJobAdmin(admin.ModelAdmin):
    list_display = ("job_id", "command", "status", "created_at", "started_at", "finished_at")
    search_fields = ("job_id", "command")


@admin.register(ReelRun)
class ReelRunAdmin(admin.ModelAdmin):
    list_display = ("run_id", "listing_title", "stock_id", "status", "updated_at")
    search_fields = ("run_id", "listing_title", "stock_id")
