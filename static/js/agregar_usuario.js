document.addEventListener('DOMContentLoaded', function() {
    const accesoUsuarios = document.getElementById('acceso_usuarios');
    const accesoProceso = document.getElementById('acceso_proceso_etl');
    const accesoSensores = document.getElementById('acceso_sensores');
    const accesoConfig = document.getElementById('acceso_configuracion');
    const adminMsg = document.getElementById('admin-message');

   accesoUsuarios.addEventListener('change', function() {
        if (accesoUsuarios.checked) {
            accesoProceso.checked = true;
            accesoSensores.checked = true;
            accesoConfig.checked = true;
            adminMsg.style.display = 'block';
            setTimeout(function() {
                adminMsg.style.display = 'none';
            }, 3500);
        }
    });
});