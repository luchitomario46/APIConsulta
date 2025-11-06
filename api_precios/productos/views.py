import cx_Oracle
from django.http import HttpResponseRedirect, JsonResponse
from decouple import config


# ------------------------- FUNCIONES AUXILIARES -------------------------
def extraer_base_alu(alu_completo):
    """
    Extrae la base del ALU (primeros 9 o 11 dígitos) según el formato
    Retorna None si el formato no es válido
    """
    if not alu_completo:
        return None
    
    alu_limpio = alu_completo.strip().replace('-', '').replace(' ', '')
    
    if len(alu_limpio) in (13, 14):  # Formato antiguo: 9 (modelo) + 3 (color) + 1-2 (talla)
        return alu_limpio[:9]
    elif len(alu_limpio) in (15, 16):  # Formato nuevo: 11 (modelo) + 3 (color) + 1-2 (talla)
        return alu_limpio[:11]
    return None


def get_db_connection():
    """Crea y retorna una conexión a la base de datos Oracle"""
    return cx_Oracle.connect(
        user=config('ORACLE_USER'),
        password=config('ORACLE_PASSWORD'),
        dsn=config('ORACLE_DSN'),
    )


# ------------------------- ENDPOINTS ORIGINALES -------------------------
def obtener_producto_tienda(request, store_no, alu):
    """
    Endpoint original - Obtiene información de un producto específico en una tienda
    GET /api/producto/tienda/<store_no>/<alu>
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                ld.store_no,
                ld.qty,
                v.alu,
                v.description1,
                p.price,
                p.price_lvl
            FROM invn_sbs_qty ld
            INNER JOIN invn_sbs_v v ON ld.item_sid = v.item_sid AND ld.qty <> 0
            INNER JOIN invn_sbs_price_v p ON ld.item_sid = p.item_sid AND p.price_lvl IN (1, 3, 5)
            WHERE ld.sbs_no = 1 AND v.alu = :alu AND ld.store_no = :store_no
        """

        cursor.execute(query, {'alu': alu, 'store_no': int(store_no)})
        results = cursor.fetchall()

        if not results:
            return JsonResponse({'error': 'Producto no encontrado'}, status=404)

        data = [{
            'store_no': row[0],
            'cantidad': row[1],
            'alu': row[2],
            'descripcion': row[3],
            'price': float(row[4]),
            'price_lvl': row[5]
        } for row in results]

        return JsonResponse(data, safe=False)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    finally:
        cursor.close()
        connection.close()


def obtener_producto(request, alu):
    """
    Endpoint original - Obtiene información de un producto en todas las tiendas
    GET /api/producto/<alu>
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                ld.store_no,
                ld.qty,
                v.alu,
                v.description1,
                p.price,
                p.price_lvl
            FROM invn_sbs_qty ld
            INNER JOIN invn_sbs_v v ON ld.item_sid = v.item_sid AND ld.qty <> 0
            INNER JOIN invn_sbs_price_v p ON ld.item_sid = p.item_sid AND p.price_lvl IN (1, 3, 5)
            WHERE ld.sbs_no = 1 AND v.alu = :alu
        """

        cursor.execute(query, {'alu': alu})
        results = cursor.fetchall()

        if not results:
            return JsonResponse({'error': 'Producto no encontrado'}, status=404)

        data = [{
            'store_no': row[0],
            'cantidad': row[1],
            'alu': row[2],
            'descripcion': row[3],
            'price': float(row[4]),
            'price_lvl': row[5]
        } for row in results]

        return JsonResponse(data, safe=False)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    finally:
        cursor.close()
        connection.close()


# ------------------------- NUEVOS ENDPOINTS VARIANTES -------------------------
def obtener_variantes_tienda(request, store_no, alu):
    """
    Nuevo Endpoint - Obtiene colores y tallas disponibles de un producto en una tienda específica
    GET /api/variantes/tienda/<store_no>/<alu>/
    """
    try:
        base_alu = extraer_base_alu(alu)
        if not base_alu:
            return JsonResponse({'error': 'Formato de ALU inválido'}, status=400)

        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                v.alu,
                CASE 
                    WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 10, 3)
                    WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 12, 3)
                END AS color_code,
                CASE 
                    WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 13)
                    WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 15)
                END AS talla,
                ld.qty
            FROM invn_sbs_qty ld
            JOIN invn_sbs_v v ON ld.item_sid = v.item_sid
            WHERE ld.sbs_no = 1
              AND ld.store_no = :store_no
              AND ld.qty > 0
              AND (
                  (LENGTH(v.alu) = 14 AND SUBSTR(v.alu, 1, 9) = :base_alu)
                  OR
                  (LENGTH(v.alu) = 16 AND SUBSTR(v.alu, 1, 11) = :base_alu)
              )
            ORDER BY color_code, talla
        """

        cursor.execute(query, {'store_no': int(store_no), 'base_alu': base_alu})
        results = cursor.fetchall()

        if not results:
            return JsonResponse({
                'error': 'No hay variantes disponibles',
                'base_alu': base_alu,
                'alu_recibido': alu
            }, status=404)

        # Procesamiento de resultados
        colores = {}
        tallas_disponibles = set()
        
        for alu_db, color_code, talla, cantidad in results:
            if color_code not in colores:
                colores[color_code] = {
                    'codigo': color_code,
                    'tallas': [],
                    'total': 0
                }
            
            colores[color_code]['tallas'].append({
                'talla': talla,
                'cantidad': cantidad,
                'alu_completo': alu_db
            })
            colores[color_code]['total'] += cantidad
            tallas_disponibles.add(talla)

        # Ordenar tallas numéricamente y alfabéticamente
        tallas_ordenadas = sorted(
            tallas_disponibles, 
            key=lambda x: (x.isdigit(), int(x) if x.isdigit() else x)
        )

        return JsonResponse({
            'base_alu': base_alu,
            'alu_recibido': alu,
            'tallas_disponibles': tallas_ordenadas,
            'colores': list(colores.values())
        }, safe=False)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    finally:
        cursor.close()
        connection.close()


def obtener_variantes_global(request, alu):
    """
    Endpoint Modificado - Busca el ALU completo en todas las tiendas
    GET /api/variantes/<alu>/
    Devuelve las tiendas donde está disponible el ALU específico
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                ld.store_no,
                ld.qty,
                v.alu,
                v.description1,
                p.price,
                p.price_lvl
            FROM invn_sbs_qty ld
            INNER JOIN invn_sbs_v v ON ld.item_sid = v.item_sid
            INNER JOIN invn_sbs_price_v p ON ld.item_sid = p.item_sid
            WHERE ld.sbs_no = 1
              AND ld.qty > 0
              AND v.alu = :alu
            ORDER BY ld.store_no
        """

        cursor.execute(query, {'alu': alu})
        results = cursor.fetchall()

        if not results:
            return JsonResponse({
                'error': 'El producto no está disponible en ninguna tienda',
                'alu_buscado': alu
            }, status=404)

        # Procesamiento de resultados simplificado
        tiendas = []
        
        for row in results:
            tiendas.append({
                'store_no': row[0],
                'cantidad': row[1],
                'alu': row[2],
                'descripcion': row[3],
                'precio': float(row[4]),
                'nivel_precio': row[5]
            })

        return JsonResponse({
            'alu_buscado': alu,
            'tiendas_disponibles': tiendas,
            'total_tiendas': len(tiendas)
        }, safe=False)

    except Exception as e:
        return JsonResponse({
            'error': str(e),
            'alu_buscado': alu
        }, status=500)
    finally:
        cursor.close()
        connection.close()

import traceback

def obtener_tallas_disponibles(request, store_no, alu):
    """
    Endpoint final que muestra TODAS las tallas disponibles (XS, S, M, L, XL)
    independientemente del ALU específico que se ingrese
    """
    try:
        # Limpieza del ALU recibido
        alu_limpio = alu.strip().replace('-', '').replace(' ', '')
        
        # Extraer la base del ALU (modelo + color) según formato
        if len(alu_limpio) in (13, 14):  # Formatos antiguos: 9 modelo + 3 color + 1-2 talla
            base_alu = alu_limpio[:12]  # 9 (modelo) + 3 (color)
            condicion = f"SUBSTR(v.alu, 1, 12) = '{base_alu}'"
            posicion_talla = 13
        elif len(alu_limpio) in (15, 16):  # Formatos nuevos: 11 modelo + 3 color + 1-2 talla
            base_alu = alu_limpio[:14]  # 11 (modelo) + 3 (color)
            condicion = f"SUBSTR(v.alu, 1, 14) = '{base_alu}'"
            posicion_talla = 15
        else:
            return JsonResponse({'error': 'Formato de ALU inválido'}, status=400)

        connection = get_db_connection()
        cursor = connection.cursor()

        # Consulta SQL optimizada para obtener TODAS las tallas
        query = f"""
            SELECT
                CASE 
                    WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 13, 2)
                    WHEN LENGTH(v.alu) = 13 THEN SUBSTR(v.alu, 13, 1)
                    WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 15, 2)
                    WHEN LENGTH(v.alu) = 15 THEN SUBSTR(v.alu, 15, 1)
                END AS talla,
                SUM(ld.qty) AS cantidad
            FROM invn_sbs_qty ld
            JOIN invn_sbs_v v ON ld.item_sid = v.item_sid
            WHERE ld.sbs_no = 1
              AND ld.store_no = :store_no
              AND ld.qty > 0
              AND (
                  LENGTH(v.alu) IN (13, 14, 15, 16)
                  AND {condicion}
              )
            GROUP BY 
                CASE 
                    WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 13, 2)
                    WHEN LENGTH(v.alu) = 13 THEN SUBSTR(v.alu, 13, 1)
                    WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 15, 2)
                    WHEN LENGTH(v.alu) = 15 THEN SUBSTR(v.alu, 15, 1)
                END
            ORDER BY 
                CASE 
                    WHEN REGEXP_LIKE(
                        CASE 
                            WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 13, 2)
                            WHEN LENGTH(v.alu) = 13 THEN SUBSTR(v.alu, 13, 1)
                            WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 15, 2)
                            WHEN LENGTH(v.alu) = 15 THEN SUBSTR(v.alu, 15, 1)
                        END, '^[0-9]+[A-Z]?$'
                    ) THEN 
                        TO_NUMBER(
                            REGEXP_SUBSTR(
                                CASE 
                                    WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 13, 2)
                                    WHEN LENGTH(v.alu) = 13 THEN SUBSTR(v.alu, 13, 1)
                                    WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 15, 2)
                                    WHEN LENGTH(v.alu) = 15 THEN SUBSTR(v.alu, 15, 1)
                                END, '^[0-9]+'
                            )
                        )
                    ELSE NULL
                END,
                CASE 
                    WHEN LENGTH(v.alu) = 14 THEN SUBSTR(v.alu, 13, 2)
                    WHEN LENGTH(v.alu) = 13 THEN SUBSTR(v.alu, 13, 1)
                    WHEN LENGTH(v.alu) = 16 THEN SUBSTR(v.alu, 15, 2)
                    WHEN LENGTH(v.alu) = 15 THEN SUBSTR(v.alu, 15, 1)
                END
        """

        cursor.execute(query, {'store_no': int(store_no)})
        results = cursor.fetchall()

        if not results:
            return JsonResponse({
                'error': 'No hay tallas disponibles',
                'base_alu': base_alu,
                'alu_recibido': alu
            }, status=404)

        # Procesar y normalizar tallas
        tallas = []
        for talla, cantidad in results:
            if talla:  # Solo agregar si hay talla
                talla_limpia = talla.strip().upper()
                # Unificar formatos (ej: 'X ' -> 'X')
                talla_limpia = talla_limpia[0] if len(talla_limpia) == 1 and talla_limpia in ['X','S','M','L'] else talla_limpia
                tallas.append({
                    'talla': talla_limpia,
                    'cantidad': int(cantidad)
                })

        return JsonResponse({
            'base_alu': base_alu,
            'alu_recibido': alu,
            'tallas': tallas
        }, safe=False)

    except Exception as e:
        return JsonResponse({
            'error': str(e),
            'alu_recibido': alu
        }, status=500)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def productos_detalle(request, alu):
    """
    Endpoint para obtener información detallada de un producto específico por su ALU
    GET /api/productos-detalle/<alu>/
    Devuelve los detalles completos del producto incluyendo atributos como temporada, colección, etc.
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT 
                pt.item_sid,
                pt.alu,
                pt.description1,
                t1.AUX_3 AS VENTANA,
                t1.AUX_7 AS TEMPORADA,
                t1.AUX_8 AS COLECCIO,
                t1.AUX_9 AS FAMILIA,
                t1.AUX_10 AS MARCA,
                t1.AUX_11 AS MODELO,
                t1.AUX_12 AS COLOR,
                t1.AUX_13 AS TALLA,
                (SELECT ST1.price 
                 FROM invn_sbs_price ST1 
                 WHERE pt.item_sid = ST1.item_sid AND ST1.price_lvl = 1) AS LPPV
            FROM invn_sbs pt
            INNER JOIN V_PROM_FITEM t1 
                ON t1.SID_ARTICULO = pt.item_sid AND t1.SBS_NO = 1
            WHERE pt.SBS_NO = 1
              AND pt.ACTIVE = 1
              AND pt.ALU = :alu
        """

        cursor.execute(query, {'alu': alu})
        result = cursor.fetchone()

        if not result:
            return JsonResponse({
                'error': 'Producto no encontrado o no activo',
                'alu_buscado': alu
            }, status=404)

        # Mapear los resultados a un diccionario
        producto = {
            'item_sid': result[0],
            'alu': result[1],
            'descripcion': result[2],
            'ventana': result[3],
            'temporada': result[4],
            'coleccion': result[5],
            'familia': result[6],
            'marca': result[7],
            'modelo': result[8],
            'color': result[9],
            'talla': result[10],
            'precio_lista': float(result[11]) if result[11] is not None else None
        }

        return JsonResponse({
            'alu_buscado': alu,
            'producto': producto
        })

    except Exception as e:
        return JsonResponse({
            'error': str(e),
            'alu_buscado': alu
        }, status=500)
    finally:
        cursor.close()
        connection.close()

from django.http import HttpResponseRedirect, Http404
import requests
import re

def truncar_alu(alu):
    """
    Trunca el código ALU según las reglas:
    - 15 o 16 caracteres → 14
    - 13 o 14 caracteres → 12
    - Otros casos → sin cambios
    """
    length = len(alu)
    if length in (15, 16):
        return alu[:14]
    elif length in (13, 14):
        return alu[:12]
    return alu

def validar_alu(alu):
    """Valida que el ALU contenga solo dígitos y tenga longitud válida"""
    return bool(re.match(r'^\d{12,16}$', alu))

def obtener_imagen_producto(request, alu):
    """
    Vista para obtener la imagen de un producto basado en su código ALU
    Redirecciona a la URL de la imagen en fotos.italmod.cl
    """
    if not validar_alu(alu):
        raise Http404("Código ALU inválido. Debe contener entre 12 y 16 dígitos")
    
    alu_truncado = truncar_alu(alu)
    url_imagen = f"http://fotos.italmod.cl/{alu_truncado}_1.jpg"
    
    return HttpResponseRedirect(url_imagen)
    