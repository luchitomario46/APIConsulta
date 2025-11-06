from django.urls import path
from . import views

urlpatterns = [
    path('tienda', views.obtener_tiendas),
]
