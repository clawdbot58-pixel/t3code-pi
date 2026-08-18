/**
 * PiAdapter — shape type for the pi provider adapter.
 *
 * The driver model ({@link ../Drivers/PiDriver}) bundles one adapter per
 * instance as a captured closure, so this module only retains the shape
 * interface as a naming anchor for the driver bundle.
 *
 * @module PiAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * PiAdapterShape — per-instance pi adapter contract.
 */
export interface PiAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
