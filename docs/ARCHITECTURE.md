# Unified Django Architecture

## Goal
Host three independent workflows under one Django server while keeping clean boundaries.

## Apps
- `apps/global_make_model_video`
- `apps/real_footage_reels`
- `apps/carsale_scraper`
- `apps/common` (shared dashboard and cross-cutting concerns)

## Layering (MVC style in Django)
- `models.py`: data layer (Model)
- `views.py`: request/response orchestration (Controller)
- `templates/...`: presentation layer (View)
- `services.py`: business rules and workflow logic
- `forms.py`: input validation

## URL Topology
- `/dashboard/`
- `/workflows/global-make-model-video/`
- `/workflows/real-footage-reels/`
- `/workflows/carsale-scraper/`

## Next Merge Plan
1. Move each existing stack logic into the corresponding `services.py` package.
2. Add background workers (Celery/RQ) per workflow.
3. Add per-workflow APIs under DRF if needed.
4. Add shared observability/logging in `apps/common`.
