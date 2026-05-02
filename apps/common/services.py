class DashboardService:
    """Shared dashboard service layer for future orchestration logic."""

    @staticmethod
    def health_summary() -> dict:
        return {
            "global_make_model_video": "ready",
            "real_footage_reels": "ready",
            "carsale_scraper": "ready",
        }
