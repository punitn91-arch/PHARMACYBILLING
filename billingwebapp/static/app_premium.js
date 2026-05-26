(function () {
    const body = document.body;
    const route = body ? (body.dataset.route || "") : "";

    function isTypingTarget(element) {
        if (!element || !(element instanceof HTMLElement)) return false;
        const tag = (element.tagName || "").toUpperCase();
        return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || element.isContentEditable;
    }

    function focusSelector(selector) {
        const element = document.querySelector(selector);
        if (!element) return false;
        element.focus();
        if (typeof element.select === "function") {
            element.select();
        }
        return true;
    }

    function clickSelector(selector) {
        const element = document.querySelector(selector);
        if (!element) return false;
        element.click();
        return true;
    }

    function submitSelector(selector) {
        const element = document.querySelector(selector);
        if (!element) return false;
        if (element instanceof HTMLFormElement) {
            element.requestSubmit();
            return true;
        }
        if (element instanceof HTMLElement) {
            element.click();
            return true;
        }
        return false;
    }

    function markSelectedAppointmentPaid() {
        const trigger = document.querySelector(".appointment-row.is-shortcut-selected [data-shortcut-pay]");
        if (!trigger) return false;
        if (trigger.disabled) return false;
        trigger.click();
        return true;
    }

    document.addEventListener("keydown", function (event) {
        if (!body) return;

        const activeElement = document.activeElement;
        const typing = isTypingTarget(activeElement);
        const lowerKey = String(event.key || "").toLowerCase();

        if (route === "/billing") {
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "s") {
                event.preventDefault();
                clickSelector("[data-shortcut-save]");
                return;
            }
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "n") {
                event.preventDefault();
                clickSelector("[data-shortcut-new]");
                return;
            }
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "p") {
                event.preventDefault();
                focusSelector("#billingPatientName");
                return;
            }
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "m") {
                event.preventDefault();
                focusSelector("#medicineSearch");
                return;
            }
        }

        if (route.indexOf("/appointments") === 0) {
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "n") {
                event.preventDefault();
                clickSelector("[data-shortcut-new-appointment]");
                return;
            }
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "p") {
                event.preventDefault();
                markSelectedAppointmentPaid();
                return;
            }
            if (!typing && lowerKey === "f") {
                if (event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    focusSelector("#appointment_search");
                    return;
                }
            }
        }

        if (route.indexOf("/medicines") === 0) {
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "m") {
                event.preventDefault();
                clickSelector("[data-shortcut-add-medicine]");
                return;
            }
            if (!typing && event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "f") {
                event.preventDefault();
                focusSelector("#medicineSearch");
                return;
            }
        }

        if (route === "/invoices") {
            if (event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "n") {
                event.preventDefault();
                window.location.assign("/billing");
                return;
            }
            if (!typing && event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "f") {
                event.preventDefault();
                focusSelector("#invoiceSearch");
                return;
            }
        }

        if (!typing && event.altKey && !event.ctrlKey && !event.metaKey && lowerKey === "g") {
            focusSelector("#globalSearchInput");
        }
    });

    if (route.indexOf("/appointments") === 0) {
        document.addEventListener("click", function (event) {
            const row = event.target.closest(".appointment-row");
            if (!row) return;
            document.querySelectorAll(".appointment-row.is-shortcut-selected").forEach(function (node) {
                node.classList.remove("is-shortcut-selected");
            });
            row.classList.add("is-shortcut-selected");
        });

        document.querySelectorAll(".appointment-row").forEach(function (row) {
            row.setAttribute("tabindex", "0");
            row.addEventListener("focus", function () {
                document.querySelectorAll(".appointment-row.is-shortcut-selected").forEach(function (node) {
                    node.classList.remove("is-shortcut-selected");
                });
                row.classList.add("is-shortcut-selected");
            });
        });
    }
})();
