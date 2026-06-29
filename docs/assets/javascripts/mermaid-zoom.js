/* mermaid-zoom.js — click a Mermaid flow diagram to open it full-screen.
 *
 * Mermaid renders to inline <svg>, which the glightbox image lightbox does not
 * capture. This adds a tiny, dependency-free zoom: click a diagram to show an
 * enlarged copy in a full-screen overlay; click the overlay (or press Esc) to
 * close. It re-binds on every page via Material's `document$` observable so it
 * keeps working with instant navigation.
 */
(function () {
  function buildOverlay(svg) {
    var overlay = document.createElement("div");
    overlay.className = "mermaid-zoom-overlay";

    var clone = svg.cloneNode(true);
    clone.removeAttribute("style");
    clone.classList.add("mermaid-zoom-svg");
    overlay.appendChild(clone);

    function close() {
      overlay.remove();
      document.removeEventListener("keydown", onKey);
    }
    function onKey(e) {
      if (e.key === "Escape") close();
    }

    overlay.addEventListener("click", close);
    document.addEventListener("keydown", onKey);
    document.body.appendChild(overlay);
  }

  function bind() {
    var diagrams = document.querySelectorAll(".md-typeset .mermaid svg");
    diagrams.forEach(function (svg) {
      if (svg.dataset.zoomBound) return;
      svg.dataset.zoomBound = "1";
      svg.style.cursor = "zoom-in";
      svg.setAttribute("title", "Click to enlarge");
      svg.addEventListener("click", function () {
        buildOverlay(svg);
      });
    });
  }

  // Material exposes `document$` when instant navigation is enabled; fall back
  // to a normal load event otherwise.
  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(bind);
  } else {
    document.addEventListener("DOMContentLoaded", bind);
  }
})();
