from django.urls import path
from . import views

urlpatterns = [
    # Endpoint para obtener ventas en una tienda entre dos fechas
    path('ventas/<str:store_no>/', views.get_ventas, name='get_ventas'),
    path('comparativo/<str:store_no>/', views.comparativo_mensual, name='comparativo_mensual'),
    # Endpoint para comparar meses entre años hasta la fecha actual
    path('comparativo-hasta-fecha/<str:store_no>/', views.comparativo_mensual_hasta_fecha, name='comparativo_mensual_hasta_fecha'),
    # Endpoint para obtener ventas diarias hasta hoy
    path('ventas-diarias/<int:store_no>', views.ventas_diarias_hasta_hoy, name='ventas-diarias'),
    #Endpoint para obtener ventas comparativas anuales por día
    path('ventas-comparativo-anual-dia/<int:store_no>/', views.ventas_comparativo_anual_dia, name='ventas_comparativo_anual_dia'),
    #Endpoint para obtener el detalle completo de ventas por SKU
    path('ventas-detalle/<store_no>', views.get_detalle_ventas, name='get_detalle_ventas'),
    # Endpoint metas de ventas
    path('ventas-metas/<int:store_no>/<int:year>/<int:month>/', views.obtener_metas, name='obtener_metas'),
    #Endpoint para obtener historico
    path('historico-ventas/<int:store_no>/', views.get_ventas_historicas_proyeccion, name='get_ventas_historicas_proyeccion'),
]