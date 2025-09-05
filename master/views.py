from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login
from django.contrib.auth.decorators import login_required
from datetime import datetime
from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.contrib.auth.hashers import make_password
from .models import ETLProcessState, ETLProcessLog, Homologacion, Nivel, Central, Parametro, ETLProcessStateCron, ETLProcessLogCron
from .forms import UsuarioForm, ProfileForm, SensorForm
from .utils import acceso_modulo_requerido, importar_excel_a_cmd, ejecutar_etl_secuencial_cron
import os
from django.conf import settings
from django.urls import reverse


def login_etl(request):
    error = None
    # Obtener el máximo de intentos desde la tabla Parámetro con ID 3
    try:
        max_intentos = int(Parametro.objects.get(pk=3).valor)
    except (Parametro.DoesNotExist, ValueError, TypeError):
        max_intentos = 3  # Valor por defecto si no existe o no es válido

    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        try:
            user_obj = User.objects.get(username=username)
            profile = user_obj.profile
            # Inicializa el campo intentos si no existe
            if not hasattr(profile, 'intentos'):
                profile.intentos = 0
                profile.save()
            if profile.bloqueado:
                error = "Tu cuenta ha sido bloqueada por intentos fallidos. Contacta al administrador."
            else:
                user = authenticate(request, username=username, password=password)
                if user is not None:
                    login(request, user)
                    profile.intentos = 0  # Resetear intentos al loguear
                    profile.save()
                    return redirect('home')
                else:
                    profile.intentos += 1
                    if profile.intentos >= max_intentos:
                        profile.bloqueado = True
                        error = "Tu cuenta ha sido bloqueada por intentos fallidos. Contacta al administrador."
                    else:
                        error = 'Usuario o contraseña incorrectos.'
                    profile.save()
        except User.DoesNotExist:
            error = 'Usuario o contraseña incorrectos.'
    context = {
        'error': error,
        'year': datetime.now().year
    }
    return render(request, 'master/login.html', context)


@login_required
def home(request):
    return render(request, 'master/home.html', {'user': request.user, 'year': datetime.now().year})


@login_required
@acceso_modulo_requerido('acceso_usuarios')
def usuarios_list(request):
    usuarios = User.objects.all().order_by('username')
    return render(request, 'master/usuarios_list.html', {'usuarios': usuarios})


@login_required
@acceso_modulo_requerido('acceso_usuarios')
def editar_usuario(request, user_id):
    usuario = get_object_or_404(User, pk=user_id)
    profile = usuario.profile
    error = None

    if request.method == 'POST':
        usuario_form = UsuarioForm(request.POST, instance=usuario)
        profile_form = ProfileForm(request.POST, instance=profile)
        if usuario_form.is_valid() and profile_form.is_valid():
            usuario_form.save()
            profile_form.save()
            # Si el campo bloqueado se desactiva, reinicia los intentos
            if not profile_form.cleaned_data.get('bloqueado', False):
                profile.intentos = 0
                profile.save()
            messages.success(request, 'Usuario actualizado correctamente.')
            return redirect('usuarios_list')
        else:
            error = "Debe completar todos los campos obligatorios correctamente."
    else:
        usuario_form = UsuarioForm(instance=usuario)
        profile_form = ProfileForm(instance=profile)

    return render(request, 'master/editar_usuario.html', {
        'usuario_form': usuario_form,
        'profile_form': profile_form,
        'usuario': usuario,
        'profile': profile,
        'error': error,
        'year': datetime.now().year,
        'user': request.user
    })

@login_required
@acceso_modulo_requerido('acceso_usuarios')
def eliminar_usuario(request, user_id):
    usuario = get_object_or_404(User, pk=user_id)
    if request.method == 'POST':
        usuario.delete()
        messages.success(request, 'Usuario eliminado correctamente.')
        return redirect('usuarios_list')
    return render(request, 'master/eliminar_usuario.html', {'usuario': usuario})


@login_required
@acceso_modulo_requerido('acceso_usuarios')
def cambiar_contrasena_usuario(request, user_id):
    usuario = get_object_or_404(User, pk=user_id)
    profile = usuario.profile
    error = None
    if request.method == 'POST':
        nueva = request.POST.get('nueva')
        confirmar = request.POST.get('confirmar')
        if not nueva or not confirmar:
            error = "Debe completar ambos campos."
        elif nueva != confirmar:
            error = "Las contraseñas no coinciden."
        else:
            usuario.password = make_password(nueva)
            usuario.save()
            # Desbloquear usuario y reiniciar intentos
            profile.bloqueado = False
            profile.intentos = 0
            profile.save()
            messages.success(request, 'Contraseña cambiada correctamente. El usuario ha sido desbloqueado.')
            return redirect('usuarios_list')
    return render(request, 'master/cambiar_contrasena_usuario.html', {'usuario': usuario, 'error': error})


@login_required
@acceso_modulo_requerido('acceso_usuarios')
def agregar_usuario(request):
    error = None
    if request.method == 'POST':
        usuario_form = UsuarioForm(request.POST)
        profile_form = ProfileForm(request.POST)
        password = request.POST.get('password')
        if usuario_form.is_valid() and profile_form.is_valid() and password:
            user = usuario_form.save(commit=False)
            user.password = make_password(password)
            user.save()
            # Crear el profile si no existe
            from .models import Profile
            profile, created = Profile.objects.get_or_create(user=user)
            for field in profile_form.cleaned_data:
                setattr(profile, field, profile_form.cleaned_data[field])
            profile.save()
            messages.success(request, 'Usuario creado correctamente.')
            return redirect('usuarios_list')
        else:
            error = "Debe completar todos los campos obligatorios y la contraseña."
    else:
        usuario_form = UsuarioForm()
        profile_form = ProfileForm()
    return render(request, 'master/agregar_usuario.html', {
        'usuario_form': usuario_form,
        'profile_form': profile_form,
        'error': error,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_proceso_etl')
def etl_procesos_list(request):
    procesos = ETLProcessStateCron.objects.all().order_by('-actualizado')
    from datetime import timedelta
    for p in procesos:
        if p.actualizado:
            p.actualizado = p.actualizado - timedelta(hours=5)
    return render(request, 'master/etl_procesos_list.html', {
        'procesos': procesos,
        'year': datetime.now().year,
        'user': request.user
    })

@login_required
@acceso_modulo_requerido('acceso_proceso_etl')
def etl_proceso_detalle(request, proceso_id):
    proceso = get_object_or_404(ETLProcessStateCron, pk=proceso_id)
    logs = ETLProcessLogCron.objects.filter(proceso=proceso).order_by('-inicio')
    return render(request, 'master/etl_proceso_detalle.html', {
        'proceso': proceso,
        'logs': logs,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_sensores')
def sensores_list(request):
    nivel_id = request.GET.get('nivel')
    nivel = None
    if nivel_id:
        sensores = Homologacion.objects.filter(nivel_id=nivel_id)
        try:
            nivel = Nivel.objects.get(pk=nivel_id)
        except Nivel.DoesNotExist:
            nivel = None
    else:
        sensores = Homologacion.objects.all()
    return render(request, 'master/sensores_list.html', {
        'sensores': sensores,
        'nivel': nivel,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_sensores')
def editar_sensor(request, sensor_id):
    sensor = get_object_or_404(Homologacion, pk=sensor_id)
    error = None
    if request.method == 'POST':
        form = SensorForm(request.POST, instance=sensor)
        if form.is_valid():
            form.save()
            messages.success(request, 'Sensor actualizado correctamente.')
            # Redirige a la vista de sensores por nivel previa
            url = reverse('sensores_list') + f'?nivel={sensor.nivel.id}'
            return redirect(url)
        else:
            error = "Debe completar todos los campos obligatorios correctamente."
    else:
        form = SensorForm(instance=sensor)
    niveles = Nivel.objects.all()
    centrales = Central.objects.all()
    return render(request, 'master/editar_sensor.html', {
        'form': form,
        'sensor': sensor,
        'niveles': niveles,
        'centrales': centrales,
        'error': error,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_sensores')
def agregar_sensor(request):
    error = None
    if request.method == 'POST':
        form = SensorForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'Sensor agregado correctamente.')
            return redirect('sensores_list')
        else:
            error = "Debe completar todos los campos obligatorios correctamente."
    else:
        form = SensorForm()
    niveles = Nivel.objects.all()
    centrales = Central.objects.all()
    return render(request, 'master/agregar_sensor.html', {
        'form': form,
        'error': error,
        'year': datetime.now().year,
        'niveles': niveles,
        'centrales': centrales,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_configuracion')
def configuracion_list(request):
    parametros = Parametro.objects.all().order_by('nombre')
    return render(request, 'master/configuracion_list.html', {
        'parametros': parametros,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_configuracion')
def editar_parametro(request, parametro_id):
    parametro = get_object_or_404(Parametro, pk=parametro_id)
    error = None
    if request.method == 'POST':
        valor = request.POST.get('valor')
        if not valor:
            error = "El valor es obligatorio."
        else:
            parametro.valor = valor
            parametro.save()
            messages.success(request, 'Parámetro actualizado correctamente.')
            return redirect('configuracion_list')
    return render(request, 'master/editar_parametro.html', {
        'parametro': parametro,
        'error': error,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_sensores')
def centrales_list(request):
    centrales = Central.objects.all().order_by('descripcion')
    return render(request, 'master/centrales_list.html', {
        'centrales': centrales,
        'year': datetime.now().year,
        'user': request.user
    })



@login_required
@acceso_modulo_requerido('acceso_sensores')
def activar_central(request, central_id):
    central = get_object_or_404(Central, pk=central_id)
    if request.method == 'POST':
        central.estado = True
        central.save()
        messages.success(request, 'Central activada correctamente.')
    return redirect('centrales_list')

@login_required
@acceso_modulo_requerido('acceso_sensores')
def desactivar_central(request, central_id):
    central = get_object_or_404(Central, pk=central_id)
    if request.method == 'POST':
        central.estado = False
        central.save()
        messages.success(request, 'Central desactivada correctamente.')
    return redirect('centrales_list')

@login_required
@acceso_modulo_requerido('acceso_sensores')
def ver_niveles(request, central_id):
    central = get_object_or_404(Central, pk=central_id)
    niveles = central.nivel_set.all()
    return render(request, 'master/niveles_list.html', {
        'central': central,
        'niveles': niveles,
        'year': datetime.now().year,
        'user': request.user
    })

@login_required
@acceso_modulo_requerido('acceso_sensores')
def activar_nivel(request, nivel_id):
    nivel = get_object_or_404(Nivel, pk=nivel_id)
    if request.method == 'POST':
        nivel.estado = True
        nivel.save()
        messages.success(request, 'Nivel activado correctamente.')
    return redirect('ver_niveles', central_id=nivel.central.id)

@login_required
@acceso_modulo_requerido('acceso_sensores')
def desactivar_nivel(request, nivel_id):
    nivel = get_object_or_404(Nivel, pk=nivel_id)
    if request.method == 'POST':
        nivel.estado = False
        nivel.save()
        messages.success(request, 'Nivel desactivado correctamente.')
    return redirect('ver_niveles', central_id=nivel.central.id)



@login_required
@acceso_modulo_requerido('acceso_proceso_etl')
def cargar_excel_cmd(request):
    error = None
    success = None
    nombre_archivo = "cmd_import.xlsx"
    ruta_destino = os.path.join(settings.MEDIA_ROOT, 'importados', nombre_archivo)
    if request.method == 'POST' and request.FILES.get('archivo'):
        archivo = request.FILES['archivo']
        # Elimina el archivo anterior si existe
        if os.path.exists(ruta_destino):
            os.remove(ruta_destino)
        os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
        # Guarda el nuevo archivo
        with open(ruta_destino, 'wb+') as destino:
            for chunk in archivo.chunks():
                destino.write(chunk)
        try:
            importar_excel_a_cmd(ruta_destino)
            success = "Archivo cargado e importado correctamente."
        except Exception as e:
            error = f"Error al importar el archivo: {e}"
    # Recarga la lista de procesos ETL y muestra el mensaje
    procesos = ETLProcessState.objects.all().order_by('-actualizado')
    return render(request, 'master/etl_procesos_list.html', {
        'procesos': procesos,
        'success': success,
        'error': error,
        'year': datetime.now().year,
        'user': request.user
    })


@login_required
@acceso_modulo_requerido('acceso_proceso_etl')
def ejecutar_etl_manual(request):
    success = None
    error = None
    if request.method == 'POST':
        try:
            ejecutar_etl_secuencial_cron()
            success = "El proceso ETL se ejecutó correctamente."
        except Exception as e:
            error = f"Error al ejecutar el proceso ETL: {e}"
    # Recarga la lista de procesos ETL y muestra el mensaje flotante
    procesos = ETLProcessStateCron.objects.all().order_by('-actualizado')
    return render(request, 'master/etl_procesos_list.html', {
        'procesos': procesos,
        'success': success,
        'error': error,
        'year': datetime.now().year,
        'user': request.user
    })