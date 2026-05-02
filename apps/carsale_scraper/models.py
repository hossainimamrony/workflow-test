from django.db import models


class CarScrapeJob(models.Model):
    marketplace = models.CharField(max_length=255)
    query = models.CharField(max_length=255)
    status = models.CharField(max_length=50, default="queued")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.marketplace} - {self.query} ({self.status})"
