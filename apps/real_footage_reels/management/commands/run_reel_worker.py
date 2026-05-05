import time

from django.core.management.base import BaseCommand

from ...services import ReelRenderService


class Command(BaseCommand):
    help = (
        "Process queued Real Footage Reels jobs outside the web request process. "
        "Use this in an Always-on task / background worker."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--once",
            action="store_true",
            help="Process available queued jobs once, then exit.",
        )
        parser.add_argument(
            "--max-jobs",
            type=int,
            default=1,
            help="When --once is used, max queued jobs to process before exit (default: 1).",
        )
        parser.add_argument(
            "--poll-interval",
            type=float,
            default=3.0,
            help="Seconds to wait between queue polls in continuous mode (default: 3).",
        )

    def handle(self, *args, **options):
        once = bool(options.get("once"))
        max_jobs = max(1, int(options.get("max_jobs", 1) or 1))
        poll_interval = max(0.5, float(options.get("poll_interval", 3.0) or 3.0))

        mode = "external" if ReelRenderService.uses_external_worker() else "inline-thread"
        self.stdout.write(self.style.NOTICE(f"Worker mode: {mode}"))
        if mode != "external":
            self.stdout.write(
                self.style.WARNING(
                    "REAL_FOOTAGE_USE_EXTERNAL_WORKER is not enabled. "
                    "Web requests may still start inline thread workers.",
                )
            )

        if once:
            processed = 0
            while processed < max_jobs and ReelRenderService.run_next_queued_job():
                processed += 1
            self.stdout.write(self.style.SUCCESS(f"Processed {processed} queued job(s)."))
            return

        self.stdout.write(self.style.SUCCESS("Queue worker started. Press Ctrl+C to stop."))
        while True:
            try:
                ran = ReelRenderService.run_next_queued_job()
                if not ran:
                    time.sleep(poll_interval)
            except KeyboardInterrupt:
                self.stdout.write(self.style.WARNING("Queue worker stopped."))
                break
