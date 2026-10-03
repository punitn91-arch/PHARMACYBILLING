/*
 * Adds the CSRF token to every state-changing request made from the browser:
 *  - POST forms (normal submit, requestSubmit and form.submit())
 *  - same-origin fetch() and XMLHttpRequest calls that are not GET/HEAD
 * The token is read from <meta name="csrf-token">.
 */
(function () {
    "use strict";
    var FIELD = "_csrf_token";
    var HEADER = "X-CSRF-Token";
    var SAFE = { GET: 1, HEAD: 1, OPTIONS: 1, TRACE: 1 };

    function token() {
        var meta = document.querySelector('meta[name="csrf-token"]');
        return meta ? meta.getAttribute("content") || "" : "";
    }

    function sameOrigin(url) {
        try {
            return new URL(url, window.location.href).origin === window.location.origin;
        } catch (_err) {
            return false;
        }
    }

    function ensureFormToken(form) {
        if (!(form instanceof HTMLFormElement)) return;
        var method = (form.getAttribute("method") || "GET").toUpperCase();
        if (SAFE[method]) return;
        var action = form.getAttribute("action") || window.location.href;
        if (!sameOrigin(action)) return;
        var value = token();
        if (!value) return;
        var input = form.querySelector('input[name="' + FIELD + '"]');
        if (!input) {
            input = document.createElement("input");
            input.type = "hidden";
            input.name = FIELD;
            form.appendChild(input);
        }
        input.value = value;
    }

    // Capture phase: runs before any page script builds FormData from the form.
    document.addEventListener("submit", function (event) {
        ensureFormToken(event.target);
    }, true);

    var nativeSubmit = HTMLFormElement.prototype.submit;
    HTMLFormElement.prototype.submit = function () {
        ensureFormToken(this);
        return nativeSubmit.apply(this, arguments);
    };

    // Forms already on the page (also covers FormData(form) built without submit).
    function tagAllForms() {
        var forms = document.querySelectorAll("form");
        for (var i = 0; i < forms.length; i++) ensureFormToken(forms[i]);
    }
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", tagAllForms);
    } else {
        tagAllForms();
    }

    if (window.fetch) {
        var nativeFetch = window.fetch;
        window.fetch = function (input, init) {
            init = init || {};
            var url = typeof input === "string" ? input : (input && input.url) || "";
            var method = (init.method || (input && input.method) || "GET").toUpperCase();
            if (!SAFE[method] && sameOrigin(url) && token()) {
                var headers = new Headers(init.headers || (input && input.headers) || {});
                if (!headers.has(HEADER)) headers.set(HEADER, token());
                init = Object.assign({}, init, { headers: headers });
            }
            return nativeFetch.call(this, input, init);
        };
    }

    var nativeOpen = XMLHttpRequest.prototype.open;
    var nativeSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function (method, url) {
        this.__csrfMethod = String(method || "GET").toUpperCase();
        this.__csrfUrl = url;
        return nativeOpen.apply(this, arguments);
    };
    XMLHttpRequest.prototype.send = function () {
        if (!SAFE[this.__csrfMethod] && sameOrigin(this.__csrfUrl) && token()) {
            try {
                this.setRequestHeader(HEADER, token());
            } catch (_err) {
                // Header already sent or request not opened; ignore.
            }
        }
        return nativeSend.apply(this, arguments);
    };

    window.appCsrfToken = token;
})();
