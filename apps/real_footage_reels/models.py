from django.db import models


class ReelRun(models.Model):
    run_id = models.CharField(max_length=64, unique=True)
    listing_title = models.CharField(max_length=255, blank=True)
    stock_id = models.CharField(max_length=128, blank=True)
    car_description = models.TextField(blank=True)
    listing_price = models.CharField(max_length=128, blank=True)
    status = models.CharField(max_length=32, default="created")
    report = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]


class ReelRenderJob(models.Model):
    job_id = models.CharField(max_length=64, unique=True)
    command = models.CharField(max_length=32, default="prepare")
    status = models.CharField(max_length=32, default="queued")
    payload = models.JSONField(default=dict, blank=True)
    result = models.JSONField(default=dict, blank=True)
    error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
