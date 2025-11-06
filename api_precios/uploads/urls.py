from django.urls import path
from . import views

urlpatterns = [
    # ... tus otras URLs ...
    path('guardar-imagen/', views.guardar_imagen, name='guardar_imagen'),
    path('ver-imagen/<str:nombre_archivo>/', views.ver_imagen, name='ver_imagen'),
]