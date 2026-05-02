from .models import VideoGenerationJob


class VideoGenerationService:
    @staticmethod
    def queue_job(*, title: str, prompt: str) -> VideoGenerationJob:
        return VideoGenerationJob.objects.create(title=title, prompt=prompt)
