import cx_Oracle
from django.http import JsonResponse
from decouple import config

def obtener_promociones(request, store_no):
    try:
        # Conexión a Oracle
        connection = cx_Oracle.connect(
            user=config('ORACLE_USER'),
            password=config('ORACLE_PASSWORD'),
            dsn=config('ORACLE_DSN'),
        )
        cursor = connection.cursor()

        query = """
            SELECT 
                e.ID,
                e.DESCRIPCION, 
                e.FECHA_INI, 
                e.FECHA_FIN, 
                e.ESTADO
            FROM 
                BES_PROM_ENC e
            INNER JOIN 
                BES_PROM_FILTER_STORE fs ON e.ID = fs.ID
            WHERE 
                fs.STORE_NO = :store_no
                AND e.ESTADO = 'Vigente'
                AND e.FECHA_FIN >= TRUNC(SYSDATE) -- Solo promociones que no han expirado
            ORDER BY 
                e.ID DESC
        """

        cursor.execute(query, store_no=int(store_no))
        results = cursor.fetchall()

        data = []
        for row in results:
            data.append({
                'id': row[0],
                'descripcion': row[1],
                'fecha_ini': row[2].strftime('%Y-%m-%d') if row[2] else None,
                'fecha_fin': row[3].strftime('%Y-%m-%d') if row[3] else None,
                'estado': row[4],
            })

        cursor.close()
        connection.close()

        if not data:
            return JsonResponse({'mensaje': 'No se encontraron promociones'}, status=404)

        return JsonResponse(data, safe=False)

    except Exception as e:
        print("ERROR EN VISTA obtener_promociones:", str(e))  # 👈 imprimimos error en consola
        return JsonResponse({'error': str(e)}, status=500)

def obtener_filtro_promociones(request):
    try:
        # Conexión a Oracle
        connection = cx_Oracle.connect(
            user=config('ORACLE_USER'),
            password=config('ORACLE_PASSWORD'),
            dsn=config('ORACLE_DSN'),
        )
        cursor = connection.cursor()

        query = """
            SELECT 
                CAB_ID, 
                COLUMNA, 
                CONDICION, 
                VALOR, 
                OPERADOR, 
                TEXTO_FILTRO 
            FROM 
                BES_PROM_DET_FILTER 
            ORDER BY 
                CAB_ID DESC
        """

        cursor.execute(query)
        results = cursor.fetchall()

        data = []
        for row in results:
            data.append({
                'cab_id': row[0],
                'columna': row[1],
                'condicion': row[2],
                'valor': row[3],
                'operador': row[4],
                'texto_filtro': row[5],
            })

        cursor.close()
        connection.close()

        if not data:
            return JsonResponse({'mensaje': 'No se encontraron filtros de promociones'}, status=404)

        return JsonResponse(data, safe=False)

    except Exception as e:
        print("ERROR EN VISTA obtener_filtro_promociones:", str(e))
        return JsonResponse({'error': str(e)}, status=500)
    
def obtener_beneficio_promo(request):
    try:
        #Conexion a Oracle
        connection = cx_Oracle.connect(
            user=config('ORACLE_USER'),
            password=config('ORACLE_PASSWORD'),
            dsn=config('ORACLE_DSN'),
        )
        cursor = connection.cursor()

        query = """
            SELECT 
                * 
            FROM 
                BES_PROM_BENEF 
            ORDER BY 
                ID DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()

        data = []
        for row in results:
            data.append({
                'id': row[0],
                'tipo_benef': row[1],
                'valor': row[2],
                'item_promo': row[3],
            })
        
        cursor.close()
        connection.close()

        if not data:
            return JsonResponse({'mensaje': 'No se encontraron beneficios de promociones'}, status=404)
        
        return JsonResponse(data, safe=False)
    
    except Exception as e:
        print("ERROR EN VISTA obtener_beneficio_promo:", str(e))
        return JsonResponse({'error': str(e)}, status=500)
    
import cx_Oracle
from django.http import JsonResponse
from decouple import config

def obtener_promociones_completas(request, store_no):
    try:
        # Conexión a Oracle
        connection = cx_Oracle.connect(
            user=config('ORACLE_USER'),
            password=config('ORACLE_PASSWORD'),
            dsn=config('ORACLE_DSN'),
        )
        cursor = connection.cursor()

        # Consulta principal para obtener promociones vigentes
        query_promociones = """
            SELECT 
                e.ID,
                e.DESCRIPCION, 
                e.FECHA_INI, 
                e.FECHA_FIN, 
                e.ESTADO
            FROM 
                BES_PROM_ENC e
            INNER JOIN 
                BES_PROM_FILTER_STORE fs ON e.ID = fs.ID
            WHERE 
                fs.STORE_NO = :store_no
                AND e.ESTADO = 'Vigente'
                AND e.FECHA_FIN >= TRUNC(SYSDATE)
            ORDER BY 
                e.ID DESC
        """

        cursor.execute(query_promociones, store_no=int(store_no))
        promociones = cursor.fetchall()

        if not promociones:
            cursor.close()
            connection.close()
            return JsonResponse({'mensaje': 'No se encontraron promociones vigentes'}, status=404)

        # Obtener IDs de promociones para las consultas adicionales
        promociones_ids = [str(promo[0]) for promo in promociones]
        ids_str = ",".join(promociones_ids)

        # Consulta para obtener beneficios
        query_beneficios = f"""
            SELECT 
                ID,
                TIPO_BENEF,
                VALOR,
                ITEM_PROMO
            FROM 
                BES_PROM_BENEF
            WHERE 
                ID IN ({ids_str})
            ORDER BY 
                ID DESC
        """

        # Consulta para obtener filtros
        query_filtros = f"""
            SELECT 
                CAB_ID, 
                COLUMNA, 
                CONDICION, 
                VALOR, 
                OPERADOR, 
                TEXTO_FILTRO
            FROM 
                BES_PROM_DET_FILTER
            WHERE 
                CAB_ID IN ({ids_str})
            ORDER BY 
                CAB_ID DESC
        """

        # Ejecutar consulta de beneficios
        cursor.execute(query_beneficios)
        beneficios = cursor.fetchall()

        # Ejecutar consulta de filtros
        cursor.execute(query_filtros)
        filtros = cursor.fetchall()

        # Organizar los datos
        data = []
        for promo in promociones:
            promo_id = promo[0]
            
            # Filtrar beneficios para esta promoción
            beneficios_promo = [{
                'tipo_benef': b[1],
                'valor': float(b[2]) if b[2] else None,
                'item_promo': b[3]
            } for b in beneficios if b[0] == promo_id]
            
            # Filtrar filtros para esta promoción
            filtros_promo = [{
                'columna': f[1],
                'condicion': f[2],
                'valor': f[3],
                'operador': f[4],
                'texto_filtro': f[5]
            } for f in filtros if f[0] == promo_id]
            
            data.append({
                'id': promo_id,
                'descripcion': promo[1],
                'fecha_ini': promo[2].strftime('%Y-%m-%d') if promo[2] else None,
                'fecha_fin': promo[3].strftime('%Y-%m-%d') if promo[3] else None,
                'estado': promo[4],
                'beneficios': beneficios_promo,
                'filtros': filtros_promo
            })

        cursor.close()
        connection.close()

        return JsonResponse(data, safe=False)

    except Exception as e:
        print("ERROR EN VISTA obtener_promociones_completas:", str(e))
        return JsonResponse({'error': str(e)}, status=500)
    
from rest_framework.decorators import api_view
from rest_framework.response import Response
import requests
from datetime import datetime
import re

# Mapeo de columnas UDF a campos del producto
UDF_TO_PRODUCT_MAP = {
    'UDF3_VALUE': 'ventana',
    'UDF7_VALUE': 'temporada',
    'UDF8_VALUE': 'coleccion',
    'UDF9_VALUE': 'familia',
    'UDF10_VALUE': 'marca',
    'UDF11_VALUE': 'modelo',
    'UDF12_VALUE': 'color',
    'UDF13_VALUE': 'talla'
}

def normalizar_valor(valor):
    """Normaliza valores para comparación: convierte a string, mayúsculas y sin espacios"""
    return str(valor).strip().upper() if valor is not None else ""

def extraer_numero_temporada(temporada):
    """Extrae el número de año de una temporada (ej: 'VER2024' -> 2024)"""
    match = re.search(r'\d{4}', temporada)
    return int(match.group()) if match else 0

def evaluar_condicion(valor_producto, condicion, valor_filtro):
    """
    Evalúa una condición entre el valor del producto y el valor del filtro
    con manejo especial para temporadas y rangos
    """
    valor_producto = normalizar_valor(valor_producto)
    valor_filtro = normalizar_valor(valor_filtro)

    # Condiciones especiales para TEMPORADA
    if condicion.upper().startswith("TEMPORADA"):
        num_producto = extraer_numero_temporada(valor_producto)
        if "-" in valor_filtro:
            inicio, fin = valor_filtro.split("-")
            num_inicio = extraer_numero_temporada(inicio)
            num_fin = extraer_numero_temporada(fin)
            return num_inicio <= num_producto <= num_fin
        else:
            num_filtro = extraer_numero_temporada(valor_filtro)
            if condicion == "TEMPORADA_IGUAL":
                return num_producto == num_filtro
            elif condicion == "TEMPORADA_MAYOR":
                return num_producto > num_filtro
            elif condicion == "TEMPORADA_MENOR":
                return num_producto < num_filtro
            else:
                return False

    # Condiciones estándar para otros campos
    if condicion == "=":
        return valor_producto == valor_filtro
    elif condicion == "!=":
        return valor_producto != valor_filtro
    elif condicion == "Starts with":
        return valor_producto.startswith(valor_filtro)
    elif condicion == "Contains":
        return valor_filtro in valor_producto
    elif condicion == "Ends with":
        return valor_producto.endswith(valor_filtro)
    elif condicion == ">":
        return valor_producto > valor_filtro
    elif condicion == "<":
        return valor_producto < valor_filtro
    elif condicion == ">=":
        return valor_producto >= valor_filtro
    elif condicion == "<=":
        return valor_producto <= valor_filtro

    return False

@api_view(['GET'])
def mejor_promocion(request, alu, tienda):
    try:
        # 1. Obtener datos del producto
        producto_url = f"http://192.168.25.6:8000/api/productos-detalle/{alu}/"
        producto_response = requests.get(producto_url)

        if producto_response.status_code != 200:
            return Response({"error": "Producto no encontrado"}, status=404)

        producto_data = producto_response.json()
        producto = producto_data['producto']
        precio_lista = float(producto.get('precio_lista', 0))

        # 2. Obtener promociones para la tienda específica
        promociones_url = f"http://127.0.0.1:8000/api/promociones_completas/{tienda}/"
        promociones_response = requests.get(promociones_url)

        if promociones_response.status_code != 200:
            return Response({"error": f"No se pudieron obtener las promociones para la tienda {tienda}"}, status=500)

        promociones = promociones_response.json()

        # 3. Evaluar cada promoción
        promociones_aplicables = []

        for promocion in promociones:
            if not promocion.get('filtros') or not promocion.get('beneficios'):
                continue

            cumple_filtros = True
            condiciones = promocion['filtros']
            condiciones_no_cumplidas = []

            for filtro in condiciones:
                columna = filtro['columna']
                condicion = filtro['condicion']
                valor_filtro = filtro['valor']
                operador = filtro.get('operador', 'AND')

                campo_producto = UDF_TO_PRODUCT_MAP.get(columna)
                if not campo_producto:
                    if operador == 'AND':
                        cumple_filtros = False
                        condiciones_no_cumplidas.append(f"{columna} no mapeada")
                        break
                    continue

                valor_producto = producto.get(campo_producto, '')

                cumple = evaluar_condicion(valor_producto, condicion, valor_filtro)

                if not cumple:
                    condiciones_no_cumplidas.append(f"{campo_producto}: {valor_producto} {condicion} {valor_filtro}")

                if operador == "AND" and not cumple:
                    cumple_filtros = False
                    break
                elif operador == "OR" and cumple:
                    cumple_filtros = True
                    break

            # SOLO agregar si NO hay condiciones no cumplidas
            if cumple_filtros and not condiciones_no_cumplidas:
                max_beneficio = 0
                for beneficio in promocion['beneficios']:
                    if beneficio['tipo_benef'] == 'P%':
                        try:
                            beneficio_valor = float(beneficio['valor'])
                            if beneficio_valor > max_beneficio:
                                max_beneficio = beneficio_valor
                        except (ValueError, TypeError):
                            continue

                if max_beneficio > 0:
                    precio_final_promo = round(precio_lista * (1 - max_beneficio / 100), 2)
                    promociones_aplicables.append({
                        'promocion_id': promocion['id'],
                        'descripcion': promocion['descripcion'],
                        'descuento': max_beneficio,
                        'precio_final': precio_final_promo,
                        'prioridad': promocion.get('prioridad', 0),
                        'fecha_inicio': promocion.get('fecha_ini', ''),
                        'fecha_fin': promocion.get('fecha_fin', ''),
                        'beneficios': promocion['beneficios']
                    })

        # Ordenar promociones aplicables por prioridad y descuento
        promociones_aplicables.sort(key=lambda x: (-x['prioridad'], -x['descuento']))

        # 4. Seleccionar la mejor promoción (primera de la lista ordenada)
        mejor_promo = promociones_aplicables[0] if promociones_aplicables else None

        # 5. Formatear respuesta - Solo incluir promociones con condiciones_no_cumplidas vacías
        promociones_filtradas = [p for p in promociones_aplicables if not p.get('condiciones_no_cumplidas', [])]
        
        response_data = {
            'producto': {
                'alu': producto['alu'],
                'descripcion': producto['descripcion'],
                'ventana': producto['ventana'],
                'temporada': producto['temporada'],
                'coleccion': producto['coleccion'],
                'familia': producto['familia'],
                'marca': producto['marca'],
                'modelo': producto['modelo'],
                'color': producto['color'],
                'talla': producto['talla'],
                'precio_lista': producto['precio_lista'],
                
            },
            'tienda': tienda,
            'mejor_promocion': mejor_promo,
            'todas_promociones_aplicables': promociones_filtradas,
            'promociones_evaluadas': len(promociones),
            'promociones_aplicables': len(promociones_filtradas),
            'fecha_consulta': datetime.now().isoformat(),
            'resumen': {
                'precio_original': precio_lista,
                'mejor_descuento': mejor_promo['descuento'] if mejor_promo else 0,
                'mejor_precio_final': mejor_promo['precio_final'] if mejor_promo else precio_lista,
                'total_promociones_disponibles': len(promociones_filtradas)
            }
        }

        return Response(response_data)

    except Exception as e:
        return Response({
            "error": str(e),
            "type": type(e).__name__,
            "details": f"Error al procesar promoción para ALU: {alu}, Tienda: {tienda}"
        }, status=500)