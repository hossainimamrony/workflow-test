from django.db import models


class VideoGenerationJob(models.Model):
    title = models.CharField(max_length=255)
    prompt = models.TextField()
    status = models.CharField(max_length=50, default="queued")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.title} ({self.status})"
