import type { CatalogPlugin, GetResourceContext, Resource } from '@data-fair/lib-common-types/catalog/index.js'
import type { AirtableConfig } from '#types'
import Airtable from 'airtable'
import { stringify } from 'csv-stringify/sync'

/**
 * Private type to symbolize a field
 */
type Field = {
  name: string,
  linkedTable?: string,
  primaryField?: string
}

/**
 * Retrieves the fields (headers) of a specific table in an Airtable base,
 * including information about links to other tables if necessary.
 *
 * @param apiKey - The Airtable API key used for authentication.
 * @param baseId - The identifier of the Airtable base to query.
 * @param tableId - The identifier of the table whose fields are to be retrieved.
 * @returns A promise resolved with an array of fields (`Field[]`) describing the table columns.
 * @throws An error if fetching tables fails or if the table or a linked table is not found.
 */
const getHeaders = async (apiKey: string, baseId: string, tableId: string): Promise<Field[]> => {
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    }
  })
  if (!response.ok) {
    const msg = `Erreur lors de la récupération des tables Airtable (${response.status} ${response.statusText}).`
    console.error(msg)
    throw new Error(msg)
  }
  const tables: any[] = (await response.json()).tables
  const table = tables.find((t) => t.id === tableId)
  if (!table) {
    const msg = `Table avec l'identifiant "${tableId}" introuvable dans la base "${baseId}".`
    console.error(msg)
    throw new Error(msg)
  }
  const fieldsFetched: any[] = table.fields

  const fields: Field[] = []
  fieldsFetched.forEach((field: { name: string, type: string, options: { linkedTableId: string } }) => {
    const f: Field = { name: field.name }
    if (field.type === 'multipleRecordLinks') {
      f.linkedTable = field.options.linkedTableId
      const linkedTable = tables.find((t) => t.id === f.linkedTable)
      if (!linkedTable) {
        const msg = `Table liée "${f.linkedTable}" introuvable pour le champ "${field.name}".`
        console.error(msg)
        throw new Error(msg)
      }
      f.primaryField = linkedTable.primaryFieldId
    }
    fields.push(f)
  })

  return fields
}

/**
 * Caches records from linked tables in Airtable.
 *
 * @param apiKey - The Airtable API key used for authentication.
 * @param baseId - The identifier of the Airtable base to query.
 * @param fields - The array of fields to check for linked tables.
 * @param cache - An optional cache object to store linked table records.
 * @returns A promise resolved with the updated cache containing linked table records.
 * @throws An error if fetching records from a linked table fails.
 */
const getLinkedTables = async (
  apiKey: string,
  baseId: string,
  fields: Field[],
  cache: Record<string, Record<string, any>> = {}
): Promise<Record<string, Record<string, any>>> => {
  for (const field of fields) {
    if (field.linkedTable && !cache[field.linkedTable]) {
      const base = new Airtable({ apiKey }).base(baseId)
      const records: Record<string, any> = {}
      await new Promise<void>((resolve, reject) => {
        base(field.linkedTable!)
          .select({
            fields: field.primaryField ? [field.primaryField] : undefined
          })
          .eachPage(
            (pageRecords, fetchNextPage) => {
              pageRecords.forEach(r => {
                const fieldNames = Object.keys(r.fields)
                if (fieldNames.length > 0) {
                  records[r.id] = r.fields[fieldNames[0]]
                }
              })
              fetchNextPage()
            },
            (err) => {
              if (err) {
                const msg = `Erreur lors de la récupération des enregistrements de la table liée "${field.linkedTable}": ${err.message || err}`
                console.error(msg)
                reject(new Error(msg))
              } else {
                resolve()
              }
            }
          )
      })
      cache[field.linkedTable] = records
    }
  }
  return cache
}

/**
 * Retrieves information about a resource thanks to its resourceId.
 * This function fetches the information and returns a Resource object.
 *
 * @param params.secrets - The secrets object containing the Airtable API key.
 * @param params.resourceId - The resource identifier in the format "baseId/tableId"
 * @returns A promise resolved with the Resource metadata.
 */

const getMetaData = async ({ secrets, resourceId }: GetResourceContext<AirtableConfig>): Promise<Resource> => {
  const [baseId, tableId] = resourceId.split('/')

  // Récupère les métadonnées de la table spécifique
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: {
      Authorization: `Bearer ${secrets.apiKey}`,
      'Content-Type': 'application/json'
    }
  })
  if (!response.ok) {
    console.error(`Failed to get table metadata: ${response.status} ${response.statusText}`)
    throw new Error('Erreur dans la récupération des métadonnées de la table')
  }

  const data = await response.json()
  const table = (data.tables || []).find((t: any) => t.id === tableId)
  if (!table) {
    throw new Error(`Table ${tableId} not found in base ${baseId}`)
  }

  // format des tables : https://airtable.com/developers/web/api/model/table-model
  return {
    id: resourceId,
    origin: `https://api.airtable.com/v0/${baseId}/${tableId}`,
    title: table.name,
    format: 'csv',
    description: table.description,
    filePath: ''
  }
}

/**
 * Downloads records from an Airtable table as a CSV file.
 *
 * This function fetches the table structure and data from Airtable using the provided API key and resource ID,
 * resolves any linked table references, and writes the resulting data to a CSV file in the specified temporary directory.
 *
 * @param params.secrets - The secrets object containing the Airtable API key.
 * @param params.resourceId - The resource identifier in the format "baseId/tableId".
 * @param params.tmpDir - The temporary directory where the CSV file will be written.
 * @returns The path to the generated CSV file.
 *
 * @remarks
 * - Linked table fields are resolved and joined with a pipe ('|') separator.
 */
const download = async ({ secrets, resourceId, tmpDir }: GetResourceContext<AirtableConfig>): Promise<string> => {
  const [baseId, tableId] = resourceId.split('/')

  const fields = await getHeaders(secrets.apiKey, baseId, tableId)
  const linkedTablesCache = await getLinkedTables(secrets.apiKey, baseId, fields)

  const base = new Airtable({ apiKey: secrets.apiKey }).base(baseId)

  const fs = await import('fs')
  const path = await import('path')
  const outputPath = path.join(tmpDir, `${tableId}.csv`)
  const writeStream = fs.createWriteStream(outputPath)

  const allHeaders: string[] = fields.map((f: Field) => f.name)
  let isFirstChunk = true

  await new Promise<void>((resolve, reject) => {
    base(tableId)
      .select()
      .eachPage(
        (records: readonly Airtable.Record<Airtable.FieldSet>[], fetchNextPage: () => void) => {
          if (records.length === 0) {
            fetchNextPage()
            return
          }

          const rows = records.map((record) => {
            const row: Record<string, any> = { ...record.fields }
            for (const field of fields) {
              if (field.linkedTable && Array.isArray(row[field.name])) {
                row[field.name] = (row[field.name] as string[])
                  .map(id => linkedTablesCache[field.linkedTable!]?.[id])
                  .filter(Boolean)
              }
              if (Array.isArray(row[field.name])) {
                row[field.name] = row[field.name].join('|')
              }
            }
            return row
          })
          const csv = stringify(rows, {
            header: isFirstChunk,
            columns: allHeaders,
          })

          writeStream.write(csv)
          isFirstChunk = false
          fetchNextPage()
        },
        function done (err: any) {
          if (err) {
            const msg = `Erreur lors de la récupération des enregistrements depuis Airtable : ${err.message || err}`
            console.error(msg)
            reject(new Error(msg))
          } else {
            writeStream.end()
            resolve()
          }
        }
      )
  })

  await new Promise<void>((resolve, reject) => {
    writeStream.on('finish', () => resolve())
    writeStream.on('error', (err) => {
      const msg = `Erreur lors de l'écriture du fichier CSV : ${err.message || err}`
      console.error(msg)
      reject(new Error(msg))
    })
  })

  return outputPath
}

/**
 * Retrieves a resource and downloads its data as a CSV file.
 *
 * @param context - The context containing secrets, resourceId, and tmpDir.
 * @returns A promise resolved with the Resource object, including the file path to the downloaded CSV.
 */
export const getResource = async (context: GetResourceContext<AirtableConfig>): ReturnType<CatalogPlugin<AirtableConfig>['getResource']> => {
  const res: Resource = await getMetaData(context)
  res.filePath = await download(context)
  return res
}
