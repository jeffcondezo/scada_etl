from django.db import models


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
    id_scada = models.CharField(max_length=50)
    cabecera_cmd = models.CharField(max_length=50)
    valor = models.FloatField()
    timestamp = models.DateTimeField()
    nivel = models.ForeignKey(Nivel, on_delete=models.CASCADE)

    def __str__(self):
        return f"{self.id_scada} - {self.cabecera_cmd} - {self.valor}"