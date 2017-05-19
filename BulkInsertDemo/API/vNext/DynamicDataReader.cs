using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

/*
 * This version of DynamicDataReader is inherited from DbDataReader, but not IDataReader, 
 * which is particularly useful in .NET Core, since it does not define IDataReader interface.
 */

namespace BulkInsertDemo.API.vNext
{
    public sealed class DynamicDataReader<T> : DbDataReader
    {
        private readonly IList<SchemaFieldDef> m_schema;
        private readonly IDictionary<string, int> m_schemaMapping;
        private readonly Func<T, string, object> m_selector;
        private IEnumerator<T> m_dataEnumerator;

        public DynamicDataReader(IList<SchemaFieldDef> schema, IEnumerable<T> data, Func<T, string, object> selector)
        {
            m_schema = schema;
            m_schemaMapping = m_schema.Select((x, i) => new { x.FieldName, Index = i }).ToDictionary(x => x.FieldName, x => x.Index);
            m_selector = selector;
            m_dataEnumerator = data.GetEnumerator();
        }

        #region IDataReader Members

        public override int Depth => 1;

        public override bool IsClosed => m_dataEnumerator == null;

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            if (IsClosed)
                throw new ObjectDisposedException(GetType().Name);
            return m_dataEnumerator.MoveNext();
        }

        public override int RecordsAffected => -1;

        public override IEnumerator GetEnumerator()
        {
            return m_dataEnumerator;
        }

        public override bool HasRows { get; } = true;

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (m_dataEnumerator != null)
                {
                    m_dataEnumerator.Dispose();
                    m_dataEnumerator = null;
                }
            }
        }

        #endregion

        #region IDataRecord Members

        public override int FieldCount => m_schema.Count;

        public override int GetOrdinal(string name)
        {
            int ordinal;
            if (!m_schemaMapping.TryGetValue(name, out ordinal))
                throw new InvalidOperationException("Unknown parameter name: " + name);
            return ordinal;
        }

        public override object GetValue(int i)
        {
            if (m_dataEnumerator == null)
                throw new ObjectDisposedException(GetType().Name);

            var value = m_selector(m_dataEnumerator.Current, m_schema[i].FieldName);

            if (value == null)
                return DBNull.Value;

            var strValue = value as string;
            if (strValue != null)
            {
                if (strValue.Length > m_schema[i].Size && m_schema[i].Size > 0)
                    strValue = strValue.Substring(0, m_schema[i].Size);
                if (m_schema[i].DataType == DbType.String)
                    return strValue;
                return SchemaFieldDef.StringToTypedValue(strValue, m_schema[i].DataType) ?? DBNull.Value;
            }

            return value;
        }

        #region Not Implemented Members

        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }

        public override object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        #endregion

        #endregion
    }
}