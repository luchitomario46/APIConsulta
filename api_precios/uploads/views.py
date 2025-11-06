from django.http import JsonResponse, HttpResponse, Http404
from django.views.decorators.csrf import csrf_exempt
import mysql.connector
import traceback
import os
from datetime import datetime
from django.conf import settings

@csrf_exempt
def guardar_imagen(request):
    """
    Endpoint para recibir y guardar imágenes desde la aplicación Flutter
    """
    if request.method == 'POST':
        try:
            # Debug: Mostrar datos recibidos
            print("\n" + "="*50)
            print("📨 Datos POST recibidos:", dict(request.POST))
            print("📦 Archivos recibidos:", dict(request.FILES) if request.FILES else "Ningún archivo recibido")
            print("="*50 + "\n")

            # Obtener datos del formulario
            tienda = request.POST.get('tienda', '').strip()  # Número de tienda (ej: "001")
            nombre = request.POST.get('nombre', '').strip()
            motivo = request.POST.get('motivo', '').strip()
            nombre_archivo = request.POST.get('nombre_archivo', '').strip()
            imagen = request.FILES.get('imagen')

            # Validación de campos obligatorios
            missing_fields = []
            if not tienda: missing_fields.append("tienda")
            if not nombre: missing_fields.append("nombre")
            if not nombre_archivo: missing_fields.append("nombre_archivo")
            if not imagen: missing_fields.append("imagen")
            
            if missing_fields:
                error_msg = f'Faltan datos obligatorios: {", ".join(missing_fields)}'
                print(f"❌ {error_msg}")
                return JsonResponse({
                    'success': False, 
                    'message': error_msg,
                    'received_data': {
                        'POST_data': dict(request.POST),
                        'FILES_received': list(request.FILES.keys()) if request.FILES else None
                    }
                }, status=400)

            # Validar extensión del archivo
            allowed_extensions = ['.jpg', '.jpeg', '.png', '.gif']
            file_extension = os.path.splitext(nombre_archivo)[1].lower()
            if file_extension not in allowed_extensions:
                return JsonResponse({
                    'success': False,
                    'message': f'Extensión de archivo no permitida. Use: {", ".join(allowed_extensions)}'
                }, status=400)

            # Crear directorio si no existe
            upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
            os.makedirs(upload_dir, exist_ok=True)

            # Generar nombre de archivo seguro
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(nombre_archivo)[0]
            safe_filename = f"{base_name}_{timestamp}{file_extension}"
            safe_filename = "".join(c for c in safe_filename if c.isalnum() or c in ['.', '-', '_']).rstrip()

            # Construir rutas
            ruta_local_completa = os.path.join(upload_dir, safe_filename)
            ruta_web = os.path.join(settings.MEDIA_URL, 'uploads', safe_filename).replace('\\', '/')

            # Guardar imagen
            print(f"💾 Guardando imagen en: {ruta_local_completa}")
            with open(ruta_local_completa, 'wb+') as destino:
                for chunk in imagen.chunks():
                    destino.write(chunk)

            # Conexión a MySQL
            print("🔌 Conectando a MySQL...")
            try:
                conn = mysql.connector.connect(
                    host="10.152.65.12",
                    user="root",
                    password="rtproSQL.,2365*",
                    database="rtpro_tools"
                )
                cursor = conn.cursor()

                # Consulta SQL con parámetros en el orden correcto
                query = """
                    INSERT INTO uploadimages 
                    (tienda, nombre, motivo, ruta_imagen, nombre_archivo, fecha)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """
                params = (tienda, nombre, motivo, ruta_web, safe_filename)
                
                print(f"⚡ Ejecutando query: {query % params}")
                cursor.execute(query, params)
                conn.commit()
                
                print("✅ Datos insertados correctamente en la base de datos")
                
            except mysql.connector.Error as err:
                print(f"❌ Error de MySQL: {err}")
                # Eliminar archivo subido si falla la DB
                if os.path.exists(ruta_local_completa):
                    os.remove(ruta_local_completa)
                return JsonResponse({
                    'success': False,
                    'message': f'Error en la base de datos: {err}',
                    'error_details': str(err)
                }, status=500)
            
            finally:
                if 'conn' in locals() and conn.is_connected():
                    cursor.close()
                    conn.close()

            print("✅ Imagen y datos guardados correctamente")
            return JsonResponse({
                'success': True, 
                'message': 'Imagen y datos guardados correctamente',
                'data': {
                    'tienda': tienda,
                    'nombre': nombre,
                    'motivo': motivo,
                    'ruta_imagen': ruta_web,
                    'nombre_archivo': safe_filename
                }
            })

        except Exception as e:
            print("❌ Error inesperado:")
            traceback.print_exc()
            return JsonResponse({
                'success': False, 
                'message': 'Error al procesar la solicitud',
                'error': str(e),
                'error_type': type(e).__name__
            }, status=500)

    return JsonResponse({
        'success': False, 
        'message': 'Método no permitido',
        'allowed_methods': ['POST']
    }, status=405)


@csrf_exempt
def ver_imagen(request, nombre_archivo):
    """
    Endpoint para servir imágenes al portal PHP
    """
    try:
        # Validar nombre de archivo
        if not nombre_archivo or not all(c.isalnum() or c in ['.', '-', '_'] for c in nombre_archivo):
            raise Http404("Nombre de archivo inválido")
        
        # Construir la ruta completa al archivo
        ruta_imagen = os.path.join(settings.MEDIA_ROOT, 'uploads', nombre_archivo)
        
        # Verificar que el archivo exista
        if not os.path.exists(ruta_imagen):
            raise Http404("La imagen no existe")
        
        # Determinar el tipo MIME
        extension = os.path.splitext(nombre_archivo)[1].lower()
        mime_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif'
        }
        content_type = mime_types.get(extension, 'application/octet-stream')
        
        # Leer y devolver el archivo
        with open(ruta_imagen, 'rb') as f:
            return HttpResponse(f.read(), content_type=content_type)
            
    except Exception as e:
        print(f"❌ Error al servir imagen: {str(e)}")
        return HttpResponse(status=500)


@csrf_exempt
def listar_imagenes(request):
    """
    Endpoint para listar todas las imágenes
    """
    try:
        print("🔍 Solicitando listado de imágenes...")
        conn = mysql.connector.connect(
            host="10.152.65.12",
            user="root",
            password="rtproSQL.,2365*",
            database="rtpro_tools"
        )
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT id, tienda, nombre, motivo, ruta_imagen, nombre_archivo, 
                   DATE_FORMAT(fecha, '%%d/%%m/%%Y %%H:%%i') as fecha_formateada
            FROM uploadimages
            ORDER BY fecha DESC
            LIMIT 100
        """)
        imagenes = cursor.fetchall()
        
        print(f"✅ Encontradas {len(imagenes)} imágenes")
        return JsonResponse({
            'success': True,
            'count': len(imagenes),
            'imagenes': imagenes
        })
        
    except mysql.connector.Error as err:
        print(f"❌ Error de MySQL: {err}")
        return JsonResponse({
            'success': False,
            'message': 'Error en la base de datos',
            'error': str(err)
        }, status=500)
        
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': 'Error al procesar la solicitud',
            'error': str(e)
        }, status=500)
        
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()