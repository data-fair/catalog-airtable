import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'
import type { CatalogPlugin, DownloadResourceContext } from '@data-fair/lib-common-types/catalog/index.js'
import type { AirtableConfig } from '#types'

const tmpDir = '/tmp'
const outputPath = path.join(tmpDir, 'tableId.csv')

describe('test the downloadResource function', () => {
  let downloadResource: CatalogPlugin['downloadResource']

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
    const plugin = (await import('../index.ts')).default as CatalogPlugin
    downloadResource = plugin.downloadResource
  })

  afterEach(() => {
    if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
    vi.resetAllMocks()
  })

  it('write a CSV with the data fetched', async () => {
    const context: DownloadResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'fake-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    const resultPath = await downloadResource(context)
    expect(resultPath).toBe(outputPath)
    expect(fs.existsSync(outputPath)).toBe(true)
    const content = fs.readFileSync(outputPath, 'utf8')
    expect(content).toBe('a,b\n1,2\n3,4\n')
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

    const context: DownloadResourceContext<AirtableConfig> = {
      secrets: { apiKey: 'invalid-api-key' },
      resourceId: 'baseId/tableId',
      tmpDir
    } as any

    await expect(async () => {
      await downloadResource(context)
    }).rejects.toThrow(/Une erreur est survenue lors du traitement de votre demande./i)
  })
})
