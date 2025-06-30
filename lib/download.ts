import type { DownloadResourceContext } from '@data-fair/lib-common-types/catalog/index.js'
import type { AirtableConfig } from '#types'
import Airtable from 'airtable'
import { stringify } from 'csv-stringify/sync'

type Field = {
  name: string,
  linkedTable?: string,
  primaryField?: string
}

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

const downloadLinkedTables = async (
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

export const downloadResource = async ({ secrets, resourceId, tmpDir }: DownloadResourceContext<AirtableConfig>): Promise<string> => {
  const [baseId, tableId] = resourceId.split('/')

  const fields = await getHeaders(secrets.apiKey, baseId, tableId)
  const linkedTablesCache = await downloadLinkedTables(secrets.apiKey, baseId, fields)

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
