import { describe, it, vi, beforeEach, afterEach, assert, expect } from 'vitest'
import plugin from '../index.ts'
import type { CatalogPlugin } from '@data-fair/types-catalogs'
const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin
const listResources = catalogPlugin.listResources

const secrets = { apiKey: 'fake-api-key' }

describe('test the list function', () => {
  beforeEach(() => {
    // Reset all mocks and modules before each test
    vi.resetAllMocks()
    vi.resetModules()
    globalThis.fetch = vi.fn()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it('lists bases when no currentFolderId', async () => {
    // @ts-ignore
    globalThis.fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        bases: [
          { id: 'base1', name: 'Base 1' },
          { id: 'base2', name: 'Base 2' }
        ]
      })
    })

    const params = {}
    // @ts-ignore
    const result = await listResources({ secrets, params })
    assert.strictEqual(JSON.stringify(result.results), JSON.stringify([
      { id: 'base1', title: 'Base 1', type: 'folder' },
      { id: 'base2', title: 'Base 2', type: 'folder' }
    ]), 'The list function should return all bases as folders when no currentFolderId is provided')
    assert.strictEqual(result.count, result.results.length, 'The result.count should return the number of element in the list')
    assert.ok(result.path.length === 0)
    assert.strictEqual(result.path.length, 0, 'The path should be empty when no currentFolderId is provided')
  })

  it('lists tables when currentFolderId is set', async () => {
    // D'abord simuler l'appel pour lister les bases (pour initialiser lastBases)
    // @ts-ignore
    globalThis.fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        bases: [
          { id: 'base1', name: 'Base 1' }
        ]
      })
    })

    // Appeler listResources pour les bases d'abord
    await listResources({ secrets, params: {}, catalogConfig: { apiKey: '' } })

    // Maintenant simuler l'appel pour lister les tables
    // @ts-ignore
    globalThis.fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        tables: [
          { id: 'table1', name: 'Table 1' },
          { id: 'table2', name: 'Table 2' },
          { id: 'table3', name: 'Table 3' }
        ]
      })
    })

    const params = { currentFolderId: 'base1' }
    const result = await listResources({ secrets, params, catalogConfig: { apiKey: '' } })
    assert.deepEqual(result.results[0], {
      id: 'base1/table1',
      title: 'Table 1',
      type: 'resource',
      format: 'csv',
      origin: 'https://airtable.com/base1/table1'
    })
    assert.strictEqual(result.count, 3)
    assert.strictEqual(result.path[0].id, 'base1')
    assert.strictEqual(result.path[0].title, 'Base 1')
  })

  it('throw an error when list with an invalid apiKey', async () => {
    // @ts-ignore
    globalThis.fetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      json: async () => ({ error: { message: 'Invalid API key' } })
    })

    await expect(async () => {
      await listResources({ secrets: { apiKey: 'invalid-key' }, params: {}, catalogConfig: { apiKey: 'wrong-api-key' } })
    }).rejects.toThrow(/Erreur dans la récupération des données depuis Airtable/i)
  })
})
