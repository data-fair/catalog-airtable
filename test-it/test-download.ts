import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'
import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { AirtableConfig } from '#types'

const tmpDir = '/tmp'
const outputPath = path.join(tmpDir, 'Table.csv')

describe('test the getResource function', () => {
  let getResource: CatalogPlugin['getResource']

  beforeEach(async () => {
    // Mock par défaut : succès
    vi.resetModules()
    vi.mock('airtable', () => ({
      default: class {
        base () {
          return () => ({
            select: () => ({
              eachPage: (cb: any, done: any) => {
                cb([{ fields: { a: 1, b: 2 } }, { fields: { a: 3, b: 4 } }], () => done())
              }
            })
          })
        }
      }
    }))
    // Mock fetch for the Airtable metadata API
    globalThis.fetch = vi.fn().mockImplementation((url: string) => {
      if (url.startsWith('https://api.airtable.com/v0/meta/bases/')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            tables: [
              { id: 'tableId', name: 'Table', fields: [{ name: 'a' }, { name: 'b' }] }
            ]
          })
        })
      }
      return Promise.reject(new Error('Unknown URL'))
    })

    const plugin = (await import('../index.ts')).default as CatalogPlugin
    getResource = plugin.getResource
  })

  afterEach(() => {
    if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
    vi.resetAllMocks()
  })

  it('write a CSV with the data fetched', async () => {
    const context: GetResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'fake-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    const resource = await getResource(context)
    const resultPath = resource?.filePath
    expect(resultPath).toBe(outputPath)
    expect(fs.existsSync(outputPath)).toBe(true)
    const content = fs.readFileSync(outputPath, 'utf8')
    expect(content).toBe('a,b\n1,2\n3,4\n')
    expect(resource?.id).toBe(context.resourceId)
    expect(resource?.format).toBe('csv')
    expect(resource?.title).toBe('Table')
  })

  it('transform array in string with \'|\' as separator', async () => {
    const context: GetResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'fake-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    // Mock Airtable to return arrays in fields
    vi.resetModules()
    vi.doMock('airtable', () => ({
      default: class {
        base () {
          return () => ({
            select: () => ({
              eachPage: (cb: any, done: any) => {
                cb([
                  { fields: { a: [1, 2], b: 'foo' } },
                  { fields: { a: [], b: 'bar' } }
                ], () => done())
              }
            })
          })
        }
      }
    }))

    const plugin = (await import('../index.ts')).default as CatalogPlugin
    const getResource = plugin.getResource

    const resource = await getResource(context)
    const resultPath = resource?.filePath
    expect(resultPath).toBe(outputPath)
    expect(fs.existsSync(outputPath)).toBe(true)
    const content = fs.readFileSync(outputPath, 'utf8')
    expect(content).toBe('a,b\n1|2,foo\n,bar\n')
  })

  it('should return an error with an invalid API key', async () => {
    vi.resetModules()
    vi.doMock('airtable', () => ({
      default: class {
        base () {
          return () => ({
            select: () => ({
              eachPage: (_cb: any, _done: any) => {
                throw new Error('Authentication invalid')
              }
            })
          })
        }
      }
    }))

    const context: GetResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'invalid-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    await expect(async () => {
      await getResource(context)
    }).rejects.toThrow(/Authentication invalid/i)
  })
})
