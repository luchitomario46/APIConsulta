from django.urls import path
from . import views

urlpatterns = [
    # Endpoints originales de productos
    path('producto/tienda/<str:store_no>/<str:alu>/', views.obtener_producto_tienda, name='producto-tienda'),
    path('producto/<str:alu>/', views.obtener_producto, name='producto-global'),
    
    # Endpoints de variantes
    path('variantes/tienda/<str:store_no>/<str:alu>/', views.obtener_variantes_tienda, name='variantes-tienda'),
    path('variantes/<str:alu>/', views.obtener_variantes_global, name='variantes-global'),
    
    # Endpoint para tallas
    path('variantes/tallas/<str:store_no>/<str:alu>/', views.obtener_tallas_disponibles, name='tallas-tienda'),

    # Endpoint para detalle de productos
    path('productos-detalle/<str:alu>/', views.productos_detalle, name='producto-detalle'),
    
    # Nuevo endpoint para imágenes de productos
    path('imagen-producto/<str:alu>/', views.obtener_imagen_producto, name='imagen-producto'),
]