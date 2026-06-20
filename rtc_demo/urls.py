"""rtc_demo URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.contrib.auth.views import LogoutView
from django.urls import path

from rtc.views import index, site_login
from rtc.sse import sse_stream, api_signal, api_join, api_hangup

urlpatterns = [
    path('', index, name="home"),
    path('login/', site_login, name="login"),
    path('logout/', LogoutView.as_view(), name="logout"),
    path('sse/<str:room_name>/', sse_stream, name="sse"),
    path('api/signal/', api_signal, name="api_signal"),
    path('api/join/', api_join, name="api_join"),
    path('api/hangup/', api_hangup, name="api_hangup"),
    path('admin/', admin.site.urls),
]

