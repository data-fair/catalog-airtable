import { getResource } from '../lib/download.ts'
import type { AirtableConfig } from '#types'
import type { GetResourceContext } from '@data-fair/types-catalogs'
import { describe, it, before, beforeEach, afterEach } from 'node:test'
import nock from 'nock'
import assert from 'node:assert'
import fs from 'node:fs'
import path from 'node:path'

const tmpDir = './data/test/downloads'

describe('getResource', () => {
  before(() => {
    nock.disableNetConnect()
    fs.mkdirSync(tmpDir, { recursive: true })
  })

  beforeEach(() => nock.cleanAll())

  afterEach(() => {
    nock.cleanAll()
    if (fs.existsSync(tmpDir)) {
      for (const f of fs.readdirSync(tmpDir)) {
        fs.unlinkSync(path.join(tmpDir, f))
      }
    }
  })

  it('writes a CSV with the data fetched', async () => {
    const tablesResponse = {
      tables: [{ id: 'tableId', name: 'Table', fields: [{ name: 'a' }, { name: 'b' }] }]
    }

    // getMetaData + getHeaders both call this endpoint
    nock('https://api.airtable.com')
      .get('/v0/meta/bases/baseId/tables')
      .twice()
      .reply(200, tablesResponse)

    // Airtable SDK fetches records
    nock('https://api.airtable.com')
      .get('/v0/baseId/tableId')
      .query(true)
      .reply(200, {
        records: [
          { id: 'rec1', createdTime: '2022-01-01T00:00:00.000Z', fields: { a: 1, b: 2 } },
          { id: 'rec2', createdTime: '2022-01-01T00:00:00.000Z', fields: { a: 3, b: 4 } }
        ]
      })

    const context: GetResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'fake-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    const resource = await getResource(context)
    const outputPath = path.join(tmpDir, 'Table.csv')

    assert.ok(resource)
    assert.strictEqual(resource.filePath, outputPath)
    assert.ok(fs.existsSync(outputPath))
    assert.strictEqual(fs.readFileSync(outputPath, 'utf8'), 'a,b\n1,2\n3,4\n')
    assert.strictEqual(resource.id, context.resourceId)
    assert.strictEqual(resource.format, 'csv')
    assert.strictEqual(resource.title, 'Table')
  })

  it("transforms arrays to strings with ' | ' as separator", async () => {
    const tablesResponse = {
      tables: [{ id: 'tableId', name: 'Table', fields: [{ name: 'a' }, { name: 'b' }] }]
    }

    nock('https://api.airtable.com')
      .get('/v0/meta/bases/baseId/tables')
      .twice()
      .reply(200, tablesResponse)

    nock('https://api.airtable.com')
      .get('/v0/baseId/tableId')
      .query(true)
      .reply(200, {
        records: [
          { id: 'rec1', createdTime: '2022-01-01T00:00:00.000Z', fields: { a: [1, 2], b: 'foo' } },
          { id: 'rec2', createdTime: '2022-01-01T00:00:00.000Z', fields: { a: [], b: 'bar' } }
        ]
      })

    const context: GetResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'fake-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    const resource = await getResource(context)
    const outputPath = path.join(tmpDir, 'Table.csv')

    assert.ok(resource)
    assert.ok(fs.existsSync(outputPath))
    assert.strictEqual(fs.readFileSync(outputPath, 'utf8'), 'a,b\n1 | 2,foo\n,bar\n')
    assert.strictEqual(resource.id, context.resourceId)
    assert.ok(resource.schema)
    assert.deepStrictEqual(resource.schema, [
      { description: undefined, key: 'a', separator: ' | ', title: 'a' },
      { description: undefined, key: 'b', title: 'b' }
    ])
  })

  it('throws an error with an invalid API key', async () => {
    nock('https://api.airtable.com')
      .get('/v0/meta/bases/baseId/tables')
      .reply(401, { error: { message: 'Invalid API key' } })

    const context: GetResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'invalid-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    await assert.rejects(
      async () => await getResource(context),
      /Erreur lors de la récupération des tables Airtable|Erreur lors de la récupération des métadonnées/i
    )
  })
})
