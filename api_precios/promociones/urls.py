from django.urls import path
from . import views

urlpatterns = [
    path('promociones/<int:store_no>/', views.obtener_promociones),
    path('promociones/obtener_promociones/<int:store_no>/', views.obtener_promociones),
    path('obtener_filtro_promociones/', views.obtener_filtro_promociones, name='obtener_filtro_promociones'),
    path('obtener_beneficio_promo/', views.obtener_beneficio_promo, name='obtener_beneficio_promo'),
    path('promociones_completas/<int:store_no>/', views.obtener_promociones_completas, name='promociones_completas'),
    path('mejor-promocion/<str:alu>/<str:tienda>/', views.mejor_promocion, name='mejor_promocion'),
]
