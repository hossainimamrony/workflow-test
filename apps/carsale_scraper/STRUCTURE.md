# Carsale Scraper - Professional Layout

## app root (Django entry layer)
- `apps.py`: Django app config
- `urls.py`: workflow-scoped routes (`/workflows/carsale-scraper/...`)
- `api_root_urls.py`: root API aliases (`/api/...`)
- `models.py`: database models
- `admin.py`: admin registration
- `carsales_cookies.json`: persisted browser cookies/session data

## core domain
- `core/workflow/matcher_runner.py`: main scraping/matching engine
- `core/workflow/state_io.py`: state files and snapshot helpers
- `core/workflow/sessions.py`: browser/session lifecycle helpers
- `core/antibot/slider_solver.py`: captcha slider automation

## web delivery
- `web/views.py`: Django view bridge for dashboard + APIs
- `web/compat.py`: request/response compatibility adapters
- `web/dashboard/runtime.py`: dashboard endpoint handlers and orchestration

## assets and runtime data
- `templates/find_my_cars_dashboard.html`: dashboard HTML
- `static/find_my_cars_dashboard.css`: dashboard CSS
- `static/find_my_cars_dashboard.js`: dashboard JS
- `find_my_cars_output/`: generated outputs and run artifacts
  - `current/`: active CSV/JSON outputs from latest runs
  - `logs/`: runtime logs (for example cloudflared)
  - `sessions/`: reusable browser storage/session state JSON
  - `state/`: internal temp/snapshots for safe atomic writes
  - `archive/`: optional place for older retained exports

## django internals
- `migrations/`: schema migrations
- `tests.py`: app tests
