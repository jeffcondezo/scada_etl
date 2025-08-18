# urls.py
from django.urls import path
from .views import *
from django.contrib.auth.views import LogoutView


urlpatterns = [
    path('login/', login_etl, name='login_etl'),
    path('', home, name='home'),
    path('logout/', LogoutView.as_view(next_page='login_etl'), name='logout'),
    path('usuarios/', usuarios_list, name='usuarios_list'),
    path('usuarios/editar/<int:user_id>/', editar_usuario, name='editar_usuario'),
    path('usuarios/eliminar/<int:user_id>/', eliminar_usuario, name='eliminar_usuario'),
    path('usuarios/cambiar-contrasena/<int:user_id>/', cambiar_contrasena_usuario, name='cambiar_contrasena_usuario'),
    path('usuarios/agregar/', agregar_usuario, name='agregar_usuario'),
    path('etl/procesos/', etl_procesos_list, name='etl_procesos_list'),
    path('etl/procesos/<int:proceso_id>/detalle/', etl_proceso_detalle, name='etl_proceso_detalle'),
    path('sensores/', sensores_list, name='sensores_list'),
    path('sensores/editar/<int:sensor_id>/', editar_sensor, name='editar_sensor'),
    path('sensores/agregar/', agregar_sensor, name='agregar_sensor'),
    path('configuracion/', configuracion_list, name='configuracion_list'),
    path('configuracion/editar/<int:parametro_id>/', editar_parametro, name='editar_parametro'),
    path('centrales/', centrales_list, name='centrales_list'),
    path('centrales/<int:central_id>/niveles/', ver_niveles, name='ver_niveles'),
    path('centrales/<int:central_id>/activar/', activar_central, name='activar_central'),
    path('centrales/<int:central_id>/desactivar/', desactivar_central, name='desactivar_central'),
    path('niveles/<int:nivel_id>/activar/', activar_nivel, name='activar_nivel'),
    path('niveles/<int:nivel_id>/desactivar/', desactivar_nivel, name='desactivar_nivel'),
    path('acceso-denegado/', lambda request: render(request, 'master/acceso_denegado.html', {'user': request.user,'year': datetime.now().year}), name='acceso_denegado'),
    path('cargar-excel-cmd/', cargar_excel_cmd, name='cargar_excel_cmd'),
]