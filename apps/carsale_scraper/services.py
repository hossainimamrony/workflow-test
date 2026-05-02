from .models import CarScrapeJob


class CarScrapeService:
    @staticmethod
    def queue_job(*, marketplace: str, query: str) -> CarScrapeJob:
        return CarScrapeJob.objects.create(marketplace=marketplace, query=query)

    @staticmethod
    def list_jobs() -> list[CarScrapeJob]:
        return list(CarScrapeJob.objects.all())

    @staticmethod
    def status_summary() -> dict:
        total = CarScrapeJob.objects.count()
        queued = CarScrapeJob.objects.filter(status="queued").count()
        completed = CarScrapeJob.objects.filter(status="completed").count()
        failed = CarScrapeJob.objects.filter(status="failed").count()
        return {
            "total": total,
            "queued": queued,
            "completed": completed,
            "failed": failed,
        }
