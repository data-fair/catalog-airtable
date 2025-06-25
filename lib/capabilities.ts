/**
 * The list of capabilities of the plugin.
 * These capabilities define the actions that can be performed with the plugin.
 * We add "as const" to ensure the type is a list of literal type, not a list of strings.
 * This allows TypeScript to check if the plugin has the required funcitons for each capability.
 */
export default [
  'import' as const,
  'search' as const,
  'importConfig' as const,
  'publishDataset' as const,
  'deletePublication' as const
]
