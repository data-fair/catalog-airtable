import { listResources } from '../lib/imports.ts'
import { describe, it, before, beforeEach, afterEach } from 'node:test'
import nock from 'nock'
import assert from 'node:assert'

describe('listResources', () => {
  before(() => nock.disableNetConnect())
  beforeEach(() => nock.cleanAll())
  afterEach(() => nock.cleanAll())

  it('lists bases when no currentFolderId', async () => {
    nock('https://api.airtable.com')
      .get('/v0/meta/bases')
      .reply(200, {
        bases: [
          { id: 'base1', name: 'Base 1' },
          { id: 'base2', name: 'Base 2' }
        ]
      })

    const result = await listResources({ secrets: { apiKey: 'any-key' }, params: {} } as any)

    assert.strictEqual(result.count, 2)
    assert.strictEqual(result.results.length, 2)
    assert.deepStrictEqual(result.results[0], { id: 'base1', title: 'Base 1', type: 'folder' })
    assert.deepStrictEqual(result.results[1], { id: 'base2', title: 'Base 2', type: 'folder' })
    assert.strictEqual(result.path.length, 0)
  })

  it('lists tables when currentFolderId is set', async () => {
    nock('https://api.airtable.com')
      .get('/v0/meta/bases/base1/tables')
      .reply(200, {
        tables: [
          { id: 'table1', name: 'Table 1' },
          { id: 'table2', name: 'Table 2' },
          { id: 'table3', name: 'Table 3' }
        ]
      })

    const result = await listResources({ secrets: { apiKey: 'any-key' }, params: { currentFolderId: 'base1' } } as any)

    assert.strictEqual(result.count, 3)
    assert.deepStrictEqual(result.results[0], {
      id: 'base1/table1',
      title: 'Table 1',
      type: 'resource',
      format: 'csv',
      origin: 'https://airtable.com/base1/table1'
    })
    assert.strictEqual(result.path.length, 1)
    assert.strictEqual(result.path[0].id, 'base1')
    assert.strictEqual(result.path[0].title, 'Base 1')
  })

  it('throws an error with an invalid API key', async () => {
    nock('https://api.airtable.com')
      .get('/v0/meta/bases')
      .reply(401, { error: { message: 'Invalid API key' } })

    await assert.rejects(
      async () => await listResources({ secrets: { apiKey: 'invalid-key' }, params: {} } as any),
      /Erreur dans la récupération des données depuis Airtable/i
    )
  })
})
