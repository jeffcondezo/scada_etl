from django.db import models
from django.contrib.auth.models import User


# Create your models here.
class Central(models.Model):
    descripcion = models.CharField(max_length=100, unique=True)
    codigo = models.CharField(max_length=50, unique=True)
    estado = models.BooleanField(default=True)

    def __str__(self):
        return self.descripcion


class Nivel(models.Model):
    descripcion = models.CharField(max_length=100)
    central = models.ForeignKey(Central, on_delete=models.CASCADE)
    codigo = models.CharField(max_length=50)
    estado = models.BooleanField(default=True)

    def __str__(self):
        return self.descripcion


class Homologacion(models.Model):
    tipo = (
        ('1', 'Numérico'),
        ('2', 'Booleano'),
    )
    id_scada = models.CharField(max_length=50, unique=True)
    cabecera_cmd = models.CharField(max_length=50, unique=True)
    nivel = models.ForeignKey(Nivel, on_delete=models.CASCADE)
    estado = models.BooleanField(default=True)
    tipo = models.CharField(max_length=50, default='1', choices=tipo)


class ScadaTemporal(models.Model):
    tipo = (
        ('1', 'extraido'),
        ('2', 'extrapolado'),
    )
    id_scada = models.CharField(max_length=50)
    cabecera_cmd = models.CharField(max_length=50)
    valor = models.FloatField()
    timestamp = models.DateTimeField()
    timestamp_utc = models.DateTimeField(blank=True, null=True)
    nivel = models.ForeignKey(Nivel, on_delete=models.CASCADE)
    tipo = models.CharField(max_length=50, default='1', choices=tipo)

    def __str__(self):
        return f"{self.id_scada} - {self.cabecera_cmd} - {self.valor}"
    
    
class Parametro(models.Model):
    nombre = models.CharField(max_length=100, unique=True)
    valor = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.nombre}: {self.valor}"
    
    class Meta:
        verbose_name_plural = "Parámetros"


class ETLProcessState(models.Model):
    ETAPAS = [
        ('importar', 'Importar'),
        ('completar', 'Completar'),
        ('exportar', 'Exportar'),
        ('comparar', 'Comparar'),
    ]
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    etapa = models.CharField(max_length=20, choices=ETAPAS)
    dia_actual = models.DateField()
    completado = models.BooleanField(default=False)
    en_ejecucion = models.BooleanField(default=False)  # Nuevo campo
    actualizado = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.etapa} - {self.dia_actual}"


class ETLProcessLog(models.Model):
    fecha = models.DateField()
    etapa = models.CharField(max_length=20)
    inicio = models.DateTimeField(auto_now_add=True)
    fin = models.DateTimeField(null=True, blank=True)
    exito = models.BooleanField(default=False)
    mensaje = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"{self.etapa} - {self.fecha} - {'OK' if self.exito else 'ERROR'}"
    


class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    acceso_usuarios = models.BooleanField(default=False)
    acceso_proceso_etl = models.BooleanField(default=False)
    acceso_sensores = models.BooleanField(default=False)
    acceso_configuracion = models.BooleanField(default=False)
    bloqueado = models.BooleanField(default=False)  # Nuevo campo para bloqueo por intentos fallidos