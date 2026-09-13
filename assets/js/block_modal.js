/** Mount only the block-owned modal settings. */
(function () {
  "use strict";
  const registry = (window.CWBlockUiBlocks = window.CWBlockUiBlocks || {});
  registry.openai_realtime_stt = {
    /** Bind settings to this modal's API context and return cleanup. */
    mount(root, api) { return window.CWRealtimeSttUi.mount(root, api); },
  };
})();
