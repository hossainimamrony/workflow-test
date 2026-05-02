# Unified Workflow Server (Django)

Professional Django base project for merging these workflows under one server:
- Global Make Model Video
- Real Footage Reels
- Carsale Scraper

## Quick Start
1. Install dependencies:
   - `python -m pip install -r requirements.txt`
2. Run migrations:
   - `python manage.py makemigrations`
   - `python manage.py migrate`
3. Run server:
   - `python manage.py runserver`
4. Open:
   - `http://127.0.0.1:8000/dashboard/`

## Settings
Default settings module is `config.settings.local`.
For production use:
- Set `DJANGO_SETTINGS_MODULE=config.settings.production`
- Set `DJANGO_SECRET_KEY`
- Set `DJANGO_ALLOWED_HOSTS`

## Structure
- `config/` project config and split settings
- `apps/` workflow apps + shared app
- `templates/` global template layout
- `docs/ARCHITECTURE.md` architectural decisions
