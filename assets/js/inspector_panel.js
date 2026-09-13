/** Mount only the block-owned inspector settings. */
(function () {
  "use strict";
  const registry = (window.CWBlockUiBlocks = window.CWBlockUiBlocks || {});
  registry.openai_realtime_sttInspectorPanel = {
    /** Bind settings to this inspector's API context and return cleanup. */
    mount(root, api) { return window.CWRealtimeSttUi.mount(root, api); },
  };
})();
