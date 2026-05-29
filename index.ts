import type CatalogPlugin from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type AirtableConfig } from '#types'
import { type AirtableCapabilities, capabilities } from './lib/capabilities.ts'

const plugin: CatalogPlugin<AirtableConfig, AirtableCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
  },

  async list (context) {
    const { list } = await import('./lib/imports.ts')
    return list(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/download.ts')
    return getResource(context)
  },

  metadata: {
    title: 'Airtable',
    capabilities
  },
  configSchema,
  assertConfigValid
}
export default plugin
