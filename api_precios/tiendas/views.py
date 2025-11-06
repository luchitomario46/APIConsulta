import mysql.connector
from django.http import JsonResponse
from decouple import config

def obtener_tiendas(request):
    # Conexión a la base de datos MySQL
    connection = mysql.connector.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        password=config('MYSQL_PASSWORD'),
        database=config('MYSQL_DATABASE'),
        port=config('MYSQL_PORT', cast=int)
    )
    
    cursor = connection.cursor(dictionary=True)
    
    # Consulta para obtener las tiendas
    query = """
                SELECT numero_rtpro, nombre_tienda, direccion_ip, almacen, codSAP, marca_tienda
                FROM datos 
                WHERE numero_rtpro NOT IN ('0', '57')
                AND activo = '1'
                ORDER BY numero_rtpro ASC
                """
    cursor.execute(query)
    
    # Obtener los resultados
    tiendas = cursor.fetchall()
    
    # Cerrar la conexión
    cursor.close()
    connection.close()
    
    # Devolver los resultados como JSON
    return JsonResponse(tiendas, safe=False)