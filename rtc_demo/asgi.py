import os

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rtc_demo.settings')

application = get_asgi_application()

# When served by a bare ASGI server (uvicorn/granian) instead of Django's
# runserver, nothing serves the static assets in development. Wrap the app in
# Django's stock static handler while DEBUG is on. (Pass-through for every
# non-static path, so the SSE stream is unaffected.)
from django.conf import settings  # noqa: E402

if settings.DEBUG:
    from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
    application = ASGIStaticFilesHandler(application)
