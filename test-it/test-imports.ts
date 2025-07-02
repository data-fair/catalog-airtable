import { describe, it, vi, beforeEach, assert, expect } from 'vitest'
import plugin from '../index.ts'
import type { CatalogPlugin } from '@data-fair/types-catalogs'
const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin
const list = catalogPlugin.list

globalThis.fetch = vi.fn()

const secrets = { apiKey: 'fake-api-key' }

describe('test the list function', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('lists bases when no currentFolderId', async () => {
    // @ts-ignore
    fetch.mockResolvedValueOnce({
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
    const result = await list({ secrets, params })
    assert.strictEqual(JSON.stringify(result.results), JSON.stringify([
      { id: 'base1', title: 'Base 1', type: 'folder' },
      { id: 'base2', title: 'Base 2', type: 'folder' }
    ]), 'The list function should return all bases as folders when no currentFolderId is provided')
    assert.strictEqual(result.count, result.results.length, 'The result.count should return the number of element in the list')
    assert.ok(result.path.length === 0)
  })

  it('lists tables when currentFolderId is set', async () => {
    // @ts-ignore
    fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        tables: [
          { id: 'table1', name: 'Table 1' },
          { id: 'table2', name: 'Table 2' },
          { id: 'table3', name: 'Table 3' }
        ]
      })
    })

    // Simule le lastBases pour le path
    // @ts-ignore
    // eslint-disable-next-line no-return-assign
    import('../lib/imports.ts').then(mod => mod['lastBases'] = [{ id: 'base1', title: 'Base 1', type: 'folder' }])

    const params = { currentFolderId: 'base1' }
    const result = await list({ secrets, params, catalogConfig: { apiKey: '' } })
    assert.strictEqual(JSON.stringify(result.results[0]), JSON.stringify(
      {
        id: 'base1/table1',
        title: 'Table 1',
        type: 'resource',
        format: 'csv',
        origin: 'https://api.airtable.com/v0/base1/table1'
      }
    ))
    assert.strictEqual(result.count, 3)
    assert.strictEqual(result.path[0].id, 'base1')
    assert.strictEqual(result.path[0].title, 'Base 1')
  })

  it('throw an error when list with an invalid apiKey', async () => {
    // @ts-ignore
    fetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      json: async () => ({ error: { message: 'Invalid API key' } })
    })

    await expect(async () => {
      await list({ secrets: { apiKey: 'invalid-key' }, params: {}, catalogConfig: { apiKey: 'wrong-api-key' } })
    }).rejects.toThrow(/Erreur dans le listage des bases \/ Clé d'API possiblement incorrecte/i)
  })
})
