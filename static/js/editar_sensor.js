document.addEventListener('DOMContentLoaded', function() {
    const centralSelect = document.getElementById('central-select');
    const nivelSelect = document.getElementById('nivel-select');

    function filtrarNiveles() {
        const centralId = centralSelect.value;
        Array.from(nivelSelect.options).forEach(function(option) {
            if (option.getAttribute('data-central') === centralId) {
                option.style.display = '';
            } else {
                option.style.display = 'none';
            }
        });
        // Selecciona el primer nivel visible si el actual no corresponde
        if (
            nivelSelect.selectedOptions.length === 0 ||
            nivelSelect.selectedOptions[0].style.display === 'none'
        ) {
            for (let option of nivelSelect.options) {
                if (option.style.display !== 'none') {
                    nivelSelect.value = option.value;
                    break;
                }
            }
        }
    }

    centralSelect.addEventListener('change', filtrarNiveles);
    filtrarNiveles(); // Filtra al cargar la página
});
