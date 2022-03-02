import _ from 'lodash';
import { each } from 'lodash-es';

// API interfaces
export interface IResultFormat {
  text: string;
  value: string;
}

export interface IDataTarget {
  target: string;
  datapoints: any[];
  refId: string;
  query: any;
}

export default class ResponseParser {
  public static parseProjects(results): IResultFormat[] {
    return ResponseParser.parseData(results, 'id', 'id');
  }

  public static parseDatasets(results): IResultFormat[] {
    return ResponseParser.parseData(results, 'datasetReference.datasetId', 'datasetReference.datasetId');
  }

  public static parseTableFields(results, filter): IResultFormat[] {
    const fields: IResultFormat[] = [];
    if (!results || results.length === 0) {
      return fields;
    }
    const res = [];
    results = ResponseParser._handleRecordFields(results, res);
    for (const fl of results) {
      if (filter.length > 0) {
        for (const flt of filter) {
          if (flt === fl.type) {
            fields.push({
              text: fl.name,
              value: fl.type,
            });
          }
        }
      } else {
        fields.push({
          text: fl.name,
          value: fl.type,
        });
      }
    }
    return fields;
  }

  public static parseDataQuery(results, format) {
    if (!results.rows) {
      return [ { data: [] }];
    }
    let res = null;
    if (format === 'time_series') {
      res = ResponseParser._toTimeSeries(results);
    }
    if (format === 'table') {
      res = ResponseParser._toTable(results);
    }
    if (format === 'var') {
      res = ResponseParser._toVar(results);
    }
    if (res === null) {
      res = [];
    }
    return res;
  }

  public static _convertValues(value, type) {
    if (['INT64', 'NUMERIC', 'FLOAT64', 'FLOAT', 'INT', 'INTEGER'].includes(type)) {
      return Number(value);
    }
    if (['TIMESTAMP'].includes(type)) {
      return new Date(Number(value) * 1000).toString();
    }
    //  No casting is required for types: DATE, DATETIME, TIME
    return value;
  }

  private static parseData(results, text, value): IResultFormat[] {
    const data: IResultFormat[] = [];
    if (!results || results.length === 0) {
      return data;
    }
    const objectTextList = text.split('.');
    const objectValueList = value.split('.');
    let itemValue;
    let itemText;
    for (let item of results) {
      item = ResponseParser.manipulateItem(item);
      itemText = item[objectTextList[0]];
      itemValue = item[objectValueList[0]];
      for (let i = 1; i < objectTextList.length; i++) {
        itemText = itemText[objectTextList[i]];
      }
      for (let i = 1; i < objectValueList.length; i++) {
        itemValue = itemValue[objectValueList[i]];
      }

      data.push({ text: itemText, value: itemValue });
    }
    return data;
  }

  private static manipulateItem(item) {
    if (item.kind === 'bigquery#table' && item.timePartitioning) {
      item.tableReference.tableId = item.tableReference.tableId + '__partitioned';
      if (item.timePartitioning.field) {
        item.tableReference.tableId += '__' + item.timePartitioning.field;
      }
    }
    return item;
  }

  private static _handleRecordFields(results, res) {
    for (const fl of results) {
      if (fl.type === 'RECORD') {
        for (const f of fl.fields) {
          if (f.type !== 'RECORD') {
            res.push({ name: fl.name + '.' + f.name, type: f.type });
          } else {
            for (const ff of f.fields) {
              ff.name = fl.name + '.' + f.name + '.' + ff.name;
            }
            res = ResponseParser._handleRecordFields(f.fields, res);
          }
        }
      } else {
        res.push({ name: fl.name, type: fl.type });
      }
    }
    return res;
  }
  private static _toTimeSeries(results) {
    let timeIndex = -1;
    let metricIndex = -1;
    const valueIndexes = [];
    for (let i = 0; i < results.schema.fields.length; i++) {
      if (timeIndex === -1 && ['DATE', 'TIMESTAMP', 'DATETIME'].includes(results.schema.fields[i].type)) {
        timeIndex = i;
      }
      if (metricIndex === -1 && results.schema.fields[i].name === 'metric') {
        metricIndex = i;
      }
      if (['INT64', 'NUMERIC', 'FLOAT64', 'FLOAT', 'INT', 'INTEGER'].includes(results.schema.fields[i].type)) {
        valueIndexes.push(i);
      }
    }
    if (timeIndex === -1) {
      throw new Error('No datetime column found in the result. The Time Series format requires a time column.');
    }
    return ResponseParser._buildDataPoints(results, timeIndex, metricIndex, valueIndexes);
  }

  private static _buildDataPoints(results, timeIndex, metricIndex, valueIndexes) {
    const data = [];
    let targetName = '';
    let metricName = '';
    let i;
    for (const row of results.rows) {
      if (row) {
        for (i = 0; i < valueIndexes.length; i++) {
          const epoch = Number(row.f[timeIndex].v) * 1000;
          const valueIndexName = results.schema.fields[valueIndexes[i]].name;
          targetName = metricIndex > -1 ? row.f[metricIndex].v.concat(' ', valueIndexName) : valueIndexName;
          metricName = metricIndex > -1 ? row.f[metricIndex].v : valueIndexName;
          if (metricIndex > -1 && valueIndexes.length === 1) {
            targetName = metricName;
          }
          const bucket = ResponseParser.findOrCreateBucket(data, targetName, metricName);
          const value = row.f[valueIndexes[i]].v === null ? null : Number(row.f[valueIndexes[i]].v)
          bucket.datapoints.push([value, epoch]);
        }
      }
    }
    return data;
  }

  private static findOrCreateBucket(data, target, metric): IDataTarget {
    let dataTarget = _.find(data, ['target', target]);
    if (!dataTarget) {
      dataTarget = { target, datapoints: [], refId: metric, query: '' };
      data.push(dataTarget);
    }

    return dataTarget;
  }

  private static _toTable(results) {
    const columns = [];
    for (const fl of results.schema.fields) {
      columns.push({
        text: fl.name,
        type: fl.type,
      });
    }
    const rows = [];
    each(results.rows, ser => {
      const r = [];
      each(ser, v => {
        for (let i = 0; i < v.length; i++) {
          const val = v[i].v ? ResponseParser._convertValues(v[i].v, columns[i].type) : '';
          r.push(val);
        }
      });
      rows.push(r);
    });
    return {
      columns,
      rows,
      type: 'table',
    };
  }

  private static _toVar(results) {
    const res = [];
    for (const row of results.rows) {
      res.push(row.f[0].v);
    }

    return _.map(res, value => {
      return { text: value };
    });
  }

  constructor(private $q) {}

  public parseTables(results): IResultFormat[] {
    return this._handleWildCardTables(
      ResponseParser.parseData(results, 'tableReference.tableId', 'tableReference.tableId')
    );
  }

  public transformAnnotationResponse(options, data) {
    const table = data.data;
    const index = {
      time: -1,
      timeend: -1, // camelCase for resulting Annotation below, but not for query column
      text: -1,
      tags: -1,
    };
    table.schema.fields.forEach((f, i) => {
      if (index.hasOwnProperty(f.name)) {
        index[f.name] = i;
      }
    });
    if (index.time < 0) {
      return this.$q.reject({
        message: 'Missing mandatory time column in annotation query.',
      });
    }
    // Could do early-out at the top, except that would skip $q.reject above
    if (!table.rows || !table.rows.length || !table.rows.map) {
      return [];
    }
    return table.rows.map(row => ({
      annotation: options.annotation,
      time: ResponseParser.guessSecondsOrMillis(row.f[index.time].v),
      timeEnd: index.timeend < 0 ? null : ResponseParser.guessSecondsOrMillis(row.f[index.timeend].v),
      text: index.text < 0 ? '' : String(row.f[index.text].v),
      tags: index.tags < 0 ? [] : String(row.f[index.tags].v).trim().split(/\s*,\s*/).filter(t =>
          t.length // skip empty/blank
      ),
    }));
  }

  /**
   * Support millis for time columns in addition to original epoch-seconds
   * by doing seconds-to-millis conversion only if the number is suspiciously
   * low enough. This supports instants between 1971 and 2285 (inclusive) in
   * either seconds or millis.
   */
  private static guessSecondsOrMillis(i) {
    let t = Math.floor(Number(i));
    // new Date(1e10) is 1970-04-26
    // new Date(1e11) is 1973-03-03
    // new Date(1e12) is 2001-09-08
    //         new Date("2022-02-22T22:22:22Z").getTime() is 1645568542000
    // new Date(1e13) is 2286-11-20
    // new Date(1e14) is 5138-11-16
    if (Math.abs(t) < 1e10) {
      t *= 1000;
    }
    return t;
  }

  private _handleWildCardTables(tables) {
    let sorted = new Map();
    let newTables = [];
    for (const t of tables) {
      const partitioned = t.text.indexOf('__partitioned');
      if (partitioned > -1) {
        t.text = t.text.substring(0, partitioned);
      }
      if (
        !t.value.match(
          /_(?:(?:20\d{2})(?:(?:(?:0[13578]|1[02])31)|(?:(?:0[1,3-9]|1[0-2])(?:29|30)))|(?:(?:20(?:0[48]|[2468][048]|[13579][26]))0229)|(?:20\d{2})(?:(?:0?[1-9])|(?:1[0-2]))(?:0?[1-9]|1\d|2[0-8]))(?!\d)$/g
        )
      ) {
        sorted = sorted.set(t.value, t.text);
      } else {
        sorted.set(
          t.text.substring(0, t.text.length - 8) + 'YYYYMMDD',
          t.text.substring(0, t.text.length - 8) + 'YYYYMMDD'
        );
      }
    }
    sorted.forEach((text, value) => {
      newTables = newTables.concat({ text, value });
    });
    return newTables;
  }
}
