document.addEventListener('DOMContentLoaded', function() {
    // Mostrar/ocultar modal CMD
    document.getElementById('btn-cmd').onclick = function() {
        document.getElementById('modal-cmd').style.display = 'block';
        return false;
    };
    document.getElementById('btn-cancel-cmd').onclick = function() {
        document.getElementById('modal-cmd').style.display = 'none';
    };
    document.getElementById('close-cmd').onclick = function() {
        document.getElementById('modal-cmd').style.display = 'none';
    };

    // Mostrar/ocultar modal Ejecución Manual ETL
    document.getElementById('btn-ejecucion').onclick = function() {
        document.getElementById('modal-ejecucion').style.display = 'block';
        return false;
    };
    document.getElementById('btn-cancel-ejecucion').onclick = function() {
        document.getElementById('modal-ejecucion').style.display = 'none';
    };
    document.getElementById('close-ejecucion').onclick = function() {
        document.getElementById('modal-ejecucion').style.display = 'none';
    };

    // Mensaje flotante desaparece solo
    setTimeout(function() {
        var msg = document.getElementById('floating-success');
        if (msg) { msg.style.display = 'none'; }
        var err = document.getElementById('floating-error');
        if (err) { err.style.display = 'none'; }
    }, 3500);
});