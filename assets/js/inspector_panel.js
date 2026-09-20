
import { mountSettings } from "./common.js";

/** Mount only the block-owned inspector settings. */

/** Bind settings to this inspector's API context and return cleanup. */
export function mount(root, api) {
  return mountSettings(root, api); 
}
