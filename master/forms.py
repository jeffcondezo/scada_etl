from django import forms
from django.contrib.auth.models import User
from .models import Profile, Homologacion

class UsuarioForm(forms.ModelForm):
    bloqueado = forms.BooleanField(required=False, label='Bloqueado')
    acceso_usuarios = forms.BooleanField(required=False, label='Acceso a Usuarios')
    acceso_proceso_etl = forms.BooleanField(required=False, label='Acceso a Proceso ETL')
    acceso_sensores = forms.BooleanField(required=False, label='Acceso a Sensores')
    acceso_configuracion = forms.BooleanField(required=False, label='Acceso a Configuración')

    class Meta:
        model = User
        fields = ['username', 'first_name', 'last_name', 'email', 'is_active', 'is_superuser']


class ProfileForm(forms.ModelForm):
    class Meta:
        model = Profile
        fields = ['bloqueado', 'acceso_usuarios', 'acceso_proceso_etl', 'acceso_sensores', 'acceso_configuracion']


class SensorForm(forms.ModelForm):
    class Meta:
        model = Homologacion
        fields = ['id_scada', 'cabecera_cmd', 'nivel', 'estado', 'tipo']