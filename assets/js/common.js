/** Block-owned settings editor shared by the STT modal and inspector. */
(function () {
  "use strict";
  const ui = (window.CWRealtimeSttUi = window.CWRealtimeSttUi || {});

  /** Save title and settings together through the block-owned validated action.
   * @param {HTMLElement} root - Mounted block-owned surface.
   * @param {object} api - Framework API with applyAction; never handles a raw key.
   * @returns {Function} Remove listeners when the owning surface unmounts.
   */
  ui.mount = function mount(root, api) {
    const fields = Array.from(root.querySelectorAll("[data-stt-setting]"));
    const title = root.querySelector("[data-stt-title]");
    const controls = title ? [title, ...fields] : fields;
    const button = root.querySelector("[data-stt-apply]");
    const feedback = root.querySelector("[data-stt-feedback]");
    /** Collect exactly the editable title/config; never send a raw API key. */
    const snapshot = () => ({
      ...(title ? { title: title.value } : {}),
      config: Object.fromEntries(fields.map((field) => [field.dataset.sttSetting, field.value])),
    });
    let saved = JSON.stringify(snapshot());
    let busy = false;
    let disposed = false;
    const changed = () => JSON.stringify(snapshot()) !== saved;
    /** Keep dirty, busy and read-only feedback consistent with the actual field values. */
    const refresh = () => {
      if (disposed || !button) return;
      button.disabled = busy || !changed() || Boolean(api.isReadOnly?.());
      button.textContent = busy ? "Application…" : "Appliquer";
    };
    /** Announce local form status without overwriting runtime diagnostics. */
    const announce = (message, error = false) => {
      if (!disposed && feedback) { feedback.textContent = message; feedback.dataset.error = String(error); }
    };
    const dirty = () => {
      announce(busy ? "Application en cours…" : changed() ? "Modifications non appliquées." : "Aucune modification.");
      refresh();
    };
    /** Validate hidden advanced fields, then save one snapshot without losing newer edits. */
    const apply = async () => {
      if (disposed || busy || !changed() || api.isReadOnly?.()) return;
      const invalid = controls.find((field) => !field.checkValidity());
      if (invalid) {
        const disclosure = invalid.closest("details");
        if (disclosure) disclosure.open = true;
        invalid.reportValidity();
        announce("Vérifiez le champ signalé avant d’appliquer.", true);
        return;
      }
      const nodePatch = snapshot();
      busy = true;
      announce("Application en cours…");
      refresh();
      try {
        const result = await api.applyAction("save_properties", nodePatch);
        if (result?.error) throw new Error(result.error);
        saved = JSON.stringify(nodePatch);
        announce(changed() ? "Enregistré ; des modifications restent à appliquer." : "Modifications appliquées. Stop puis Run pour les nouveaux réglages.");
      } catch (error) {
        announce(error.message || "Échec de l'enregistrement.", true);
      } finally {
        busy = false;
        refresh();
      }
    };
    for (const field of controls) { field.addEventListener("input", dirty); field.addEventListener("change", dirty); }
    button?.addEventListener("click", apply);
    refresh();
    return () => {
      disposed = true;
      for (const field of controls) { field.removeEventListener("input", dirty); field.removeEventListener("change", dirty); }
      button?.removeEventListener("click", apply);
    };
  };
})();
