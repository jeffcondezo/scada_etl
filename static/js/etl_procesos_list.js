setTimeout(function() {
            var msg = document.getElementById('floating-success');
            if (msg) { msg.style.display = 'none'; }
            var err = document.getElementById('floating-error');
            if (err) { err.style.display = 'none'; }
        }, 3500);
