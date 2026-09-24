import { withProperties } from "./properties.js";


import { mountSettings } from "./common.js";

/** Mount only the block-owned inspector settings. */

/** Bind settings to this inspector's API context and return cleanup. */
function mountOwned(root, api) {
  return mountSettings(root, api); 
}

/** Keep the block behavior and add properties-only accessibility. */
export function mount(root, ...args) {
  return withProperties(mountOwned).call(this, root, ...args);
}
