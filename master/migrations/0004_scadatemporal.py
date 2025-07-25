# Generated by Django 4.2.7 on 2025-07-01 04:52

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('master', '0003_central_estado_homologacion_tipo_nivel_estado'),
    ]

    operations = [
        migrations.CreateModel(
            name='ScadaTemporal',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('id_scada', models.CharField(max_length=50, unique=True)),
                ('cabecera_cmd', models.CharField(max_length=50, unique=True)),
                ('valor', models.FloatField()),
                ('timestamp', models.DateTimeField()),
                ('nivel', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='master.nivel')),
            ],
        ),
    ]
