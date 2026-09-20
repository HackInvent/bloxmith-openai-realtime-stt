
import { mountSettings } from "./common.js";

/** Mount only the block-owned modal settings. */

/** Bind settings to this modal's API context and return cleanup. */
export function mount(root, api) {
  return mountSettings(root, api); 
}
